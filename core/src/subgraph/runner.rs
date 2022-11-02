use crate::subgraph::context::IndexingContext;
use crate::subgraph::error::BlockProcessingError;
use crate::subgraph::inputs::IndexingInputs;
use crate::subgraph::state::IndexingState;
use crate::subgraph::stream::new_block_stream;
use atomic_refcell::AtomicRefCell;
use graph::blockchain::block_stream::{BlockStreamEvent, BlockWithTriggers, FirehoseCursor};
use graph::blockchain::{Block, Blockchain, DataSource, TriggerFilter as _};
use graph::components::{
    store::ModificationsAndCache,
    subgraph::{CausalityRegion, MappingError, ProofOfIndexing, SharedProofOfIndexing},
};
use graph::data::store::scalar::Bytes;
use graph::data::subgraph::{
    schema::{SubgraphError, SubgraphHealth, POI_OBJECT},
    SubgraphFeature,
};
use graph::prelude::*;
use graph::prelude::chrono::Local;
use graph::util::{backoff::ExponentialBackoff, lfu_cache::LfuCache};
use std::convert::TryFrom;
use std::sync::Arc;
use std::time::{Duration, Instant};

const MINUTE: Duration = Duration::from_secs(60);

const SKIP_PTR_UPDATES_THRESHOLD: Duration = Duration::from_secs(60 * 5);

pub struct SubgraphRunner<C, T>
where
    C: Blockchain,
    T: RuntimeHostBuilder<C>,
{
    ctx: IndexingContext<C, T>,
    state: IndexingState,
    inputs: Arc<IndexingInputs<C>>,
    logger: Logger,
    metrics: RunnerMetrics,
}

impl<C, T> SubgraphRunner<C, T>
where
    C: Blockchain,
    T: RuntimeHostBuilder<C>,
{
    pub fn new(
        inputs: IndexingInputs<C>,
        ctx: IndexingContext<C, T>,
        logger: Logger,
        metrics: RunnerMetrics,
    ) -> Self {
        Self {
            inputs: Arc::new(inputs),
            ctx,
            state: IndexingState {
                should_try_unfail_non_deterministic: true,
                synced: false,
                skip_ptr_updates_timer: Instant::now(),
                backoff: ExponentialBackoff::new(MINUTE * 2, ENV_VARS.subgraph_error_retry_ceil),
                entity_lfu_cache: LfuCache::new(),
            },
            logger,
            metrics,
        }
    }

    pub async fn run(mut self) -> Result<(), Error> {
        // println!("fn run");
        // If a subgraph failed for deterministic reasons, before start indexing, we first
        // revert the deployment head. It should lead to the same result since the error was
        // deterministic.
        if let Some(current_ptr) = self.inputs.store.block_ptr() {
            if let Some(parent_ptr) = self
                .inputs
                .triggers_adapter
                .parent_ptr(&current_ptr)
                .await?
            {
                // This reverts the deployment head to the parent_ptr if
                // deterministic errors happened.
                //
                // There's no point in calling it if we have no current or parent block
                // pointers, because there would be: no block to revert to or to search
                // errors from (first execution).
                let _outcome = self
                    .inputs
                    .store
                    .unfail_deterministic_error(&current_ptr, &parent_ptr)
                    .await?;
            }
        }

        loop {
            debug!(self.logger, "Starting or restarting subgraph");

            let block_stream_canceler = CancelGuard::new();
            let block_stream_cancel_handle = block_stream_canceler.handle();

            let mut block_stream = new_block_stream(&self.inputs, &self.ctx.filter)
                .await?
                .map_err(CancelableError::Error)
                .cancelable(&block_stream_canceler, || Err(CancelableError::Cancel));

            // Keep the stream's cancel guard around to be able to shut it down when the subgraph
            // deployment is unassigned
            self.ctx
                .instances
                .write()
                .unwrap()
                .insert(self.inputs.deployment.id, block_stream_canceler);

            debug!(self.logger, "Starting block stream");

            // Process events from the stream as long as no restart is needed
            loop {
                let mut events = Vec::new();
                for i in 0..8{
                    let event = {
                        let _section = self.metrics.stream.stopwatch.start_section("scan_blocks");
    
                        block_stream.next().await
                    };
                    events.push(event);

                }
            


                // TODO: move cancel handle to the Context
                // This will require some code refactor in how the BlockStream is created
                match self
                    .handle_stream_event(events, &block_stream_cancel_handle)
                    .await?
                {
                    Action::Continue => continue,
                    Action::Stop => {
                        info!(self.logger, "Stopping subgraph");
                        self.inputs.store.flush().await?;
                        return Ok(());
                    }
                    Action::Restart => break,
                };
            }
        }
    }

    /// Processes a block and returns the updated context and a boolean flag indicating
    /// whether new dynamic data sources have been added to the subgraph.
    async fn process_block(
        &mut self,
        block_stream_cancel_handle: &CancelHandle,
        blocks: Vec<BlockWithTriggers<C>>,
        mut firehose_cursor: Vec<FirehoseCursor>,
    ) -> Result<Action, BlockProcessingError> {
        // println!("fn process_block");
        let mut blocks_vec = Vec::new();
        let mut triggers_vec = Vec::new();
        let mut block_ptrs = Vec::new();

        let mut logger = self.logger.new(o!(
            "block_number" => format!("{:?}", blocks[0].ptr().number),
            "block_hash" => format!("{}", blocks[0].ptr().hash)
        ));

        let proof_of_indexing = None;

        for i in 0..blocks.len(){
            let triggers = blocks[i].trigger_data.clone();
            let block = Arc::new(blocks[i].block.clone());
            let block_ptr = blocks[i].ptr();
    
            logger = self.logger.new(o!(
                    "block_number" => format!("{:?}", block_ptr.number),
                    "block_hash" => format!("{}", block_ptr.hash)
            ));
    
            if triggers.len() == 1 {
                debug!(&logger, "1 candidate trigger in this block");
            } else {
                debug!(
                    &logger,
                    "{} candidate triggers in this block",
                    triggers.len()
                );
            }

            triggers_vec.push(triggers);
            blocks_vec.push(block);
    
            // let proof_of_indexing = if self.inputs.store.supports_proof_of_indexing().await? {
            //     Some(Arc::new(AtomicRefCell::new(ProofOfIndexing::new(
            //         block_ptr.number,
            //         self.ctx.instance.poi_version.clone(),
            //     ))))
            // } else {
            //     None
            // };

            block_ptrs.push(block_ptr);
        }


        // There are currently no other causality regions since offchain data is not supported.
        let causality_region = CausalityRegion::from_network(self.ctx.instance.network());

        // Process events one after the other, passing in entity operations
        // collected previously to every new event being processed
        let mut block_states = match self
            .process_triggers(&proof_of_indexing, &blocks_vec, triggers_vec, &causality_region)
            .await
        {
            // Triggers processed with no errors or with only deterministic errors.
            Ok(block_states) => block_states,

            // Some form of unknown or non-deterministic error ocurred.
            Err(MappingError::Unknown(e)) => return Err(BlockProcessingError::Unknown(e)),
            Err(MappingError::PossibleReorg(e)) => {
                info!(logger,
                    "Possible reorg detected, retrying";
                    "error" => format!("{:#}", e),
                );

                // In case of a possible reorg, we want this function to do nothing and restart the
                // block stream so it has a chance to detect the reorg.
                //
                // The `state` is unchanged at this point, except for having cleared the entity cache.
                // Losing the cache is a bit annoying but not an issue for correctness.
                //
                // See also b21fa73b-6453-4340-99fb-1a78ec62efb1.
                return Ok(Action::Restart);
            }
        };

        // If new data sources have been created, and static filters are not in use, it is necessary
        // to restart the block stream with the new filters.
        let mut needs_restart = false;

        // This loop will:
        // 1. Instantiate created data sources.
        // 2. Process those data sources for the current block.
        // Until no data sources are created or MAX_DATA_SOURCES is hit.

        // Note that this algorithm processes data sources spawned on the same block _breadth
        // first_ on the tree implied by the parent-child relationship between data sources. Only a
        // very contrived subgraph would be able to observe this.
        let mut block_states_modified = Vec::new();
        let mut has_errors = false;

        for i in 0..block_states.len(){

            let mut block_state = block_states.pop().unwrap();
            if (block_state.has_created_data_sources() && !self.inputs.static_filters)==true{
                needs_restart = true;
            }
            while block_state.has_created_data_sources() {
                // Instantiate dynamic data sources, removing them from the block state.
                let (data_sources, runtime_hosts) =
                    self.create_dynamic_data_sources(block_state.drain_created_data_sources())?;
    
                let filter = C::TriggerFilter::from_data_sources(data_sources.iter());
    
                // Reprocess the triggers from this block that match the new data sources
                let block_with_triggers = self
                    .inputs
                    .triggers_adapter
                    .triggers_in_block(&logger, blocks_vec[i].as_ref().clone(), &filter)
                    .await?;
    
                let triggers = block_with_triggers.trigger_data;
    
                if triggers.len() == 1 {
                    info!(
                        &logger,
                        "1 trigger found in this block for the new data sources"
                    );
                } else if triggers.len() > 1 {
                    info!(
                        &logger,
                        "{} triggers found in this block for the new data sources",
                        triggers.len()
                    );
                }
    
                // Add entity operations for the new data sources to the block state
                // and add runtimes for the data sources to the subgraph instance.
                self.persist_dynamic_data_sources(&mut block_state.entity_cache, data_sources);
    
                // Process the triggers in each host in the same order the
                // corresponding data sources have been created.
                // for trigger in triggers {
                // let temp_block = Vec::new();
                // // temp_block.push()
                // block_state = self
                //     .ctx
                //     .instance
                //     .trigger_processor
                //     .process_trigger(
                //         &logger,
                //         &runtime_hosts,
                //         &blocks_vec[i],
                //         &triggers,
                //         block_state,
                //         &proof_of_indexing,
                //         &causality_region,
                //         &self.inputs.debug_fork,
                //         &self.metrics.subgraph,
                //     )
                //     .await
                //     .map_err(|e| {
                //         // This treats a `PossibleReorg` as an ordinary error which will fail the subgraph.
                //         // This can cause an unnecessary subgraph failure, to fix it we need to figure out a
                //         // way to revert the effect of `create_dynamic_data_sources` so we may return a
                //         // clean context as in b21fa73b-6453-4340-99fb-1a78ec62efb1.
                //         match e {
                //             MappingError::PossibleReorg(e) | MappingError::Unknown(e) => {
                //                 BlockProcessingError::Unknown(e)
                //             }
                //         }
                //     })?;
                // }
            }

            if block_state.has_errors()==true{
                has_errors = true;
            }

            block_states_modified.push(block_state);

        }

        // let has_errors = block_state.has_errors();
        let is_non_fatal_errors_active = self
            .inputs
            .features
            .contains(&SubgraphFeature::NonFatalErrors);

        // Apply entity operations and advance the stream

        // Avoid writing to store if block stream has been canceled
        if block_stream_cancel_handle.is_canceled() {
            return Err(BlockProcessingError::Canceled);
        }

        // if let Some(proof_of_indexing) = proof_of_indexing {
        //     let proof_of_indexing = Arc::try_unwrap(proof_of_indexing).unwrap().into_inner();
        //     update_proof_of_indexing(
        //         proof_of_indexing,
        //         &self.metrics.host.stopwatch,
        //         &self.inputs.deployment.hash,
        //         &mut block_state.entity_cache,
        //     )
        //     .await?;
        // }


        for i in 0..block_states_modified.len(){
            let block_state = block_states_modified.pop().unwrap();
            let block_ptr = block_ptrs.pop().unwrap();

            let end_dt = Local::now();
            println!(
                "block {}  end  time: {} ",
                &block_ptr.number,
                end_dt.format("%Y-%m-%d %H:%M:%S:%.3f").to_string()
            );

            let firehose_cursor_item = firehose_cursor[i].clone();
            let section = self
                .metrics
                .host
                .stopwatch
                .start_section("as_modifications");
            let ModificationsAndCache {
                modifications: mut mods,
                data_sources,
                entity_lfu_cache: cache,
            } = block_state
                .entity_cache
                .as_modifications()
                .map_err(|e| BlockProcessingError::Unknown(e.into()))?;
            section.end();

            // Put the cache back in the state, asserting that the placeholder cache was not used.
            // assert!(self.state.entity_lfu_cache.is_empty());
            self.state.entity_lfu_cache = cache;

            if !mods.is_empty() {
                info!(&logger, "Applying {} entity operation(s)", mods.len());
            }

            let err_count = block_state.deterministic_errors.len();
            for (i, e) in block_state.deterministic_errors.iter().enumerate() {
                let message = format!("{:#}", e).replace("\n", "\t");
                error!(&logger, "Subgraph error {}/{}", i + 1, err_count;
                    "error" => message,
                    "code" => LogCode::SubgraphSyncingFailure
                );
            }

            // Transact entity operations into the store and update the
            // subgraph's block stream pointer
            let _section = self.metrics.host.stopwatch.start_section("transact_block");
            let start = Instant::now();

            let store = &self.inputs.store;

            // If a deterministic error has happened, make the PoI to be the only entity that'll be stored.
            if has_errors && !is_non_fatal_errors_active {
                let is_poi_entity =
                    |entity_mod: &EntityModification| entity_mod.entity_key().entity_type.is_poi();
                mods.retain(is_poi_entity);
                // Confidence check
                assert!(
                    mods.len() == 1,
                    "There should be only one PoI EntityModification"
                );
            }

            let BlockState {
                deterministic_errors,
                ..
            } = block_state;

            let first_error = deterministic_errors.first().cloned();

            store
                .transact_block_operations(
                    block_ptr,
                    firehose_cursor_item,
                    mods,
                    &self.metrics.host.stopwatch,
                    data_sources,
                    deterministic_errors,
                    self.inputs.manifest_idx_and_name.clone(),
                )
                .await
                .context("Failed to transact block operations")?;

            // For subgraphs with `nonFatalErrors` feature disabled, we consider
            // any error as fatal.
            //
            // So we do an early return to make the subgraph stop processing blocks.
            //
            // In this scenario the only entity that is stored/transacted is the PoI,
            // all of the others are discarded.
            if has_errors && !is_non_fatal_errors_active {
                // Only the first error is reported.
                return Err(BlockProcessingError::Deterministic(first_error.unwrap()));
            }

            let elapsed = start.elapsed().as_secs_f64();
            self.metrics
                .subgraph
                .block_ops_transaction_duration
                .observe(elapsed);

            // To prevent a buggy pending version from replacing a current version, if errors are
            // present the subgraph will be unassigned.
            if has_errors && !ENV_VARS.disable_fail_fast && !store.is_deployment_synced().await? {
                store
                    .unassign_subgraph()
                    .map_err(|e| BlockProcessingError::Unknown(e.into()))?;

                // Use `Canceled` to avoiding setting the subgraph health to failed, an error was
                // just transacted so it will be already be set to unhealthy.
                return Err(BlockProcessingError::Canceled);
            }
        }







        match needs_restart {
            true => Ok(Action::Restart),
            false => Ok(Action::Continue),
        }
    }

    async fn process_triggers(
        &mut self,
        proof_of_indexing: &SharedProofOfIndexing,
        block: &Vec<Arc<C::Block>>,
        triggers: Vec<Vec<C::TriggerData>>,
        causality_region: &str,
    ) -> Result<Vec<BlockState<C>>, MappingError> {
        use graph::blockchain::TriggerData;

        // let (tx,rx)=channel();
        let mut block_states = Vec::new();
        // println!("fn process_triggers");
        for i in 0..block.len(){


            let mut block_state = BlockState::new(
                self.inputs.store.clone(),
                std::mem::take(&mut self.state.entity_lfu_cache),
            );

            block_states.push(block_state);
        }


        let block_states = self.ctx.instance.
                    process_trigger(
                        &self.logger,
                        &block, 
                        &triggers, 
                        block_states, 
                        proof_of_indexing, 
                        causality_region, 
                        &self.inputs.debug_fork,
                        &self.metrics.subgraph)
                        .await
                        .map_err(move |mut e| {
                            let error_context = triggers[0][0].error_context();
                            if !error_context.is_empty() {
                                e = e.context(error_context);
                            }
                            e.context("failed to process trigger".to_string())
                        })?;
        

        

        
        Ok(block_states)
    }

    fn create_dynamic_data_sources(
        &mut self,
        created_data_sources: Vec<DataSourceTemplateInfo<C>>,
    ) -> Result<(Vec<C::DataSource>, Vec<Arc<T::Host>>), Error> {
        let mut data_sources = vec![];
        let mut runtime_hosts = vec![];

        for info in created_data_sources {
            // Try to instantiate a data source from the template
            let data_source = C::DataSource::try_from(info)?;

            // Try to create a runtime host for the data source
            let host = self.ctx.instance.add_dynamic_data_source(
                &self.logger,
                data_source.clone(),
                self.inputs.templates.clone(),
                self.metrics.host.clone(),
            )?;

            match host {
                Some(host) => {
                    data_sources.push(data_source);
                    runtime_hosts.push(host);
                }
                None => {
                    warn!(
                        self.logger,
                        "no runtime hosted created, there is already a runtime host instantiated for \
                        this data source";
                        "name" => &data_source.name(),
                        "address" => &data_source.address()
                        .map(|address| hex::encode(address))
                        .unwrap_or("none".to_string()),
                    )
                }
            }
        }

        Ok((data_sources, runtime_hosts))
    }

    fn persist_dynamic_data_sources(
        &mut self,
        entity_cache: &mut EntityCache,
        data_sources: Vec<C::DataSource>,
    ) {
        if !data_sources.is_empty() {
            debug!(
                self.logger,
                "Creating {} dynamic data source(s)",
                data_sources.len()
            );
        }

        // Add entity operations to the block state in order to persist
        // the dynamic data sources
        for data_source in data_sources.iter() {
            debug!(
                self.logger,
                "Persisting data_source";
                "name" => &data_source.name(),
                "address" => &data_source.address().map(|address| hex::encode(address)).unwrap_or("none".to_string()),
            );
            entity_cache.add_data_source(data_source);
        }

        // Merge filters from data sources into the block stream builder
        self.ctx.filter.extend(data_sources.iter());
    }
}

impl<C, T> SubgraphRunner<C, T>
where
    C: Blockchain,
    T: RuntimeHostBuilder<C>,
{
    async fn handle_stream_event(
        &mut self,
        events: Vec<Option<Result<BlockStreamEvent<C>, CancelableError<Error>>>>,
        cancel_handle: &CancelHandle,
    ) -> Result<Action, Error> {
        // println!("fn handle_stream_event");
        let mut blocks = Vec::new();
        let mut cursors = Vec::new();
        let mut action = Action::Continue;
        for event in events{
            action = match event {
                Some(Ok(BlockStreamEvent::ProcessBlock(block, cursor))) => {
                    blocks.push(block);
                    cursors.push(cursor);
                    // self.handle_process_block(block, cursor, cancel_handle)
                    //     .await?
                    Action::Continue
                }
                Some(Ok(BlockStreamEvent::Revert(revert_to_ptr, cursor))) => {
                    self.handle_revert(revert_to_ptr, cursor).await?
                }
                // Log and drop the errors from the block_stream
                // The block stream will continue attempting to produce blocks
                Some(Err(e)) => self.handle_err(e, cancel_handle).await?,
                // If the block stream ends, that means that there is no more indexing to do.
                // Typically block streams produce indefinitely, but tests are an example of finite block streams.
                None => Action::Stop,
            };
        }



        action = self.handle_process_block(blocks, cursors, cancel_handle).await?;


        Ok(action)
    }
}

enum Action {
    Continue,
    Stop,
    Restart,
}

#[async_trait]
trait StreamEventHandler<C: Blockchain> {
    async fn handle_process_block(
        &mut self,
        mut blocks: Vec<BlockWithTriggers<C>>,
        mut cursors: Vec<FirehoseCursor>,
        cancel_handle: &CancelHandle,
    ) -> Result<Action, Error>;
    async fn handle_revert(
        &mut self,
        revert_to_ptr: BlockPtr,
        cursor: FirehoseCursor,
    ) -> Result<Action, Error>;
    async fn handle_err(
        &mut self,
        err: CancelableError<Error>,
        cancel_handle: &CancelHandle,
    ) -> Result<Action, Error>;
}

#[async_trait]
impl<C, T> StreamEventHandler<C> for SubgraphRunner<C, T>
where
    C: Blockchain,
    T: RuntimeHostBuilder<C>,
{
    async fn handle_process_block(
        &mut self,
        mut blocks: Vec<BlockWithTriggers<C>>,
        cursor: Vec<FirehoseCursor>,
        cancel_handle: &CancelHandle,
    ) -> Result<Action, Error> {
        // println!("fn handle_process_block");
        let mut blocks_havingtrigger = Vec::new();

        let mut trigger_count_for_blocks = 0;
        let mut block_ptr = blocks[0].ptr();

        for i in 0..blocks.len(){
            let block = blocks.pop().unwrap();


            block_ptr = block.ptr();
            self.metrics
                .stream
                .deployment_head
                .set(block_ptr.number as f64);
            if block.trigger_count()>0{
                trigger_count_for_blocks += block.trigger_count();
                blocks_havingtrigger.push(block);
            }
        }
        
        // if block.trigger_count() > 0 {
        //     self.metrics
        //         .subgraph
        //         .block_trigger_count
        //         .observe(block.trigger_count() as f64);
        // }

        // if block.trigger_count() == 0
        //     && self.state.skip_ptr_updates_timer.elapsed() <= SKIP_PTR_UPDATES_THRESHOLD
        //     && !self.state.synced
        //     && !close_to_chain_head(
        //         &block_ptr,
        //         self.inputs.chain.chain_store().cached_head_ptr().await?,
        //         // The "skip ptr updates timer" is ignored when a subgraph is at most 1000 blocks
        //         // behind the chain head.
        //         1000,
        //     )
        // {
        //     return Ok(Action::Continue);
        // } else {
        //     self.state.skip_ptr_updates_timer = Instant::now();
        // }


        if trigger_count_for_blocks > 0 {
            self.metrics
                .subgraph
                .block_trigger_count
                .observe(trigger_count_for_blocks as f64);
        }
        if trigger_count_for_blocks == 0{
            return Ok(Action::Continue);
        }



        let start = Instant::now();

        // let res = self.process_block(&cancel_handle, block, cursor).await;


        let res = self.process_block(&cancel_handle, blocks_havingtrigger, cursor).await;



        let end_dt = Local::now();
        // println!(
        //     "block {}  end  time: {} ",
        //     &block_ptr.number,
        //     end_dt.format("%Y-%m-%d %H:%M:%S:%.3f").to_string()
        // );




        let elapsed = start.elapsed().as_secs_f64();
        self.metrics
            .subgraph
            .block_processing_duration
            .observe(elapsed);

        match res {
            Ok(action) => {
                // Once synced, no need to try to update the status again.
                if !self.state.synced
                    && close_to_chain_head(
                        &block_ptr,
                        self.inputs.chain.chain_store().cached_head_ptr().await?,
                        // We consider a subgraph synced when it's at most 1 block behind the
                        // chain head.
                        1,
                    )
                {
                    // Updating the sync status is an one way operation.
                    // This state change exists: not synced -> synced
                    // This state change does NOT: synced -> not synced
                    self.inputs.store.deployment_synced()?;

                    // Stop trying to update the sync status.
                    self.state.synced = true;

                    // Stop recording time-to-sync metrics.
                    self.metrics.stream.stopwatch.disable();
                }

                // Keep trying to unfail subgraph for everytime it advances block(s) until it's
                // health is not Failed anymore.
                if self.state.should_try_unfail_non_deterministic {
                    // If the deployment head advanced, we can unfail
                    // the non-deterministic error (if there's any).
                    let outcome = self
                        .inputs
                        .store
                        .unfail_non_deterministic_error(&block_ptr)?;

                    if let UnfailOutcome::Unfailed = outcome {
                        // Stop trying to unfail.
                        self.state.should_try_unfail_non_deterministic = false;
                        self.metrics.stream.deployment_failed.set(0.0);
                        self.state.backoff.reset();
                    }
                }

                if let Some(stop_block) = &self.inputs.stop_block {
                    if block_ptr.number >= *stop_block {
                        info!(self.logger, "stop block reached for subgraph");
                        return Ok(Action::Stop);
                    }
                }

                if matches!(action, Action::Restart) {
                    // Cancel the stream for real
                    self.ctx
                        .instances
                        .write()
                        .unwrap()
                        .remove(&self.inputs.deployment.id);

                    // And restart the subgraph
                    return Ok(Action::Restart);
                }

                return Ok(Action::Continue);
            }
            Err(BlockProcessingError::Canceled) => {
                debug!(self.logger, "Subgraph block stream shut down cleanly");
                return Ok(Action::Stop);
            }

            // Handle unexpected stream errors by marking the subgraph as failed.
            Err(e) => {
                // Clear entity cache when a subgraph fails.
                //
                // This is done to be safe and sure that there's no state that's
                // out of sync from the database.
                //
                // Without it, POI changes on failure would be kept in the entity cache
                // and be transacted incorrectly in the next run.
                self.state.entity_lfu_cache = LfuCache::new();

                self.metrics.stream.deployment_failed.set(1.0);

                let message = format!("{:#}", e).replace("\n", "\t");
                let err = anyhow!("{}, code: {}", message, LogCode::SubgraphSyncingFailure);
                let deterministic = e.is_deterministic();

                let error = SubgraphError {
                    subgraph_id: self.inputs.deployment.hash.clone(),
                    message,
                    block_ptr: Some(block_ptr),
                    handler: None,
                    deterministic,
                };

                match deterministic {
                    true => {
                        // Fail subgraph:
                        // - Change status/health.
                        // - Save the error to the database.
                        self.inputs
                            .store
                            .fail_subgraph(error)
                            .await
                            .context("Failed to set subgraph status to `failed`")?;

                        return Err(err);
                    }
                    false => {
                        // Shouldn't fail subgraph if it's already failed for non-deterministic
                        // reasons.
                        //
                        // If we don't do this check we would keep adding the same error to the
                        // database.
                        let should_fail_subgraph =
                            self.inputs.store.health().await? != SubgraphHealth::Failed;

                        if should_fail_subgraph {
                            // Fail subgraph:
                            // - Change status/health.
                            // - Save the error to the database.
                            self.inputs
                                .store
                                .fail_subgraph(error)
                                .await
                                .context("Failed to set subgraph status to `failed`")?;
                        }

                        // Retry logic below:

                        // Cancel the stream for real.
                        self.ctx
                            .instances
                            .write()
                            .unwrap()
                            .remove(&self.inputs.deployment.id);

                        let message = format!("{:#}", e).replace("\n", "\t");
                        error!(self.logger, "Subgraph failed with non-deterministic error: {}", message;
                            "attempt" => self.state.backoff.attempt,
                            "retry_delay_s" => self.state.backoff.delay().as_secs());

                        // Sleep before restarting.
                        self.state.backoff.sleep_async().await;

                        self.state.should_try_unfail_non_deterministic = true;

                        // And restart the subgraph.
                        return Ok(Action::Restart);
                    }
                }
            }
        }
    }

    async fn handle_revert(
        &mut self,
        revert_to_ptr: BlockPtr,
        cursor: FirehoseCursor,
    ) -> Result<Action, Error> {
        // Current deployment head in the database / WritableAgent Mutex cache.
        //
        // Safe unwrap because in a Revert event we're sure the subgraph has
        // advanced at least once.
        let subgraph_ptr = self.inputs.store.block_ptr().unwrap();
        if revert_to_ptr.number >= subgraph_ptr.number {
            info!(&self.logger, "Block to revert is higher than subgraph pointer, nothing to do"; "subgraph_ptr" => &subgraph_ptr, "revert_to_ptr" => &revert_to_ptr);
            return Ok(Action::Continue);
        }

        info!(&self.logger, "Reverting block to get back to main chain"; "subgraph_ptr" => &subgraph_ptr, "revert_to_ptr" => &revert_to_ptr);

        if let Err(e) = self
            .inputs
            .store
            .revert_block_operations(revert_to_ptr, cursor)
            .await
        {
            error!(&self.logger, "Could not revert block. Retrying"; "error" => %e);

            // Exit inner block stream consumption loop and go up to loop that restarts subgraph
            return Ok(Action::Restart);
        }

        self.metrics
            .stream
            .reverted_blocks
            .set(subgraph_ptr.number as f64);
        self.metrics
            .stream
            .deployment_head
            .set(subgraph_ptr.number as f64);

        // Revert the in-memory state:
        // - Remove hosts for reverted dynamic data sources.
        // - Clear the entity cache.
        //
        // Note that we do not currently revert the filters, which means the filters
        // will be broader than necessary. This is not ideal for performance, but is not
        // incorrect since we will discard triggers that match the filters but do not
        // match any data sources.
        self.ctx.instance.revert_data_sources(subgraph_ptr.number);
        self.state.entity_lfu_cache = LfuCache::new();

        Ok(Action::Continue)
    }

    async fn handle_err(
        &mut self,
        err: CancelableError<Error>,
        cancel_handle: &CancelHandle,
    ) -> Result<Action, Error> {
        if cancel_handle.is_canceled() {
            debug!(&self.logger, "Subgraph block stream shut down cleanly");
            return Ok(Action::Stop);
        }

        debug!(
            &self.logger,
            "Block stream produced a non-fatal error";
            "error" => format!("{}", err),
        );

        Ok(Action::Continue)
    }
}

/// Transform the proof of indexing changes into entity updates that will be
/// inserted when as_modifications is called.
async fn update_proof_of_indexing(
    proof_of_indexing: ProofOfIndexing,
    stopwatch: &StopwatchMetrics,
    deployment_id: &DeploymentHash,
    entity_cache: &mut EntityCache,
) -> Result<(), Error> {
    let _section_guard = stopwatch.start_section("update_proof_of_indexing");

    let mut proof_of_indexing = proof_of_indexing.take();

    for (causality_region, stream) in proof_of_indexing.drain() {
        // Create the special POI entity key specific to this causality_region
        let entity_key = EntityKey {
            subgraph_id: deployment_id.clone(),
            entity_type: POI_OBJECT.to_owned(),
            entity_id: causality_region,
        };

        // Grab the current digest attribute on this entity
        let prev_poi =
            entity_cache
                .get(&entity_key)
                .map_err(Error::from)?
                .map(|entity| match entity.get("digest") {
                    Some(Value::Bytes(b)) => b.clone(),
                    _ => panic!("Expected POI entity to have a digest and for it to be bytes"),
                });

        // Finish the POI stream, getting the new POI value.
        let updated_proof_of_indexing = stream.pause(prev_poi.as_deref());
        let updated_proof_of_indexing: Bytes = (&updated_proof_of_indexing[..]).into();

        // Put this onto an entity with the same digest attribute
        // that was expected before when reading.
        let new_poi_entity = entity! {
            id: entity_key.entity_id.clone(),
            digest: updated_proof_of_indexing,
        };

        entity_cache.set(entity_key, new_poi_entity)?;
    }

    Ok(())
}

/// Checks if the Deployment BlockPtr is at least X blocks behind to the chain head.
fn close_to_chain_head(
    deployment_head_ptr: &BlockPtr,
    chain_head_ptr: Option<BlockPtr>,
    n: BlockNumber,
) -> bool {
    matches!((deployment_head_ptr, &chain_head_ptr), (b1, Some(b2)) if b1.number >= (b2.number - n))
}

#[test]
fn test_close_to_chain_head() {
    let offset = 1;

    let block_0 = BlockPtr::try_from((
        "bd34884280958002c51d3f7b5f853e6febeba33de0f40d15b0363006533c924f",
        0,
    ))
    .unwrap();
    let block_1 = BlockPtr::try_from((
        "8511fa04b64657581e3f00e14543c1d522d5d7e771b54aa3060b662ade47da13",
        1,
    ))
    .unwrap();
    let block_2 = BlockPtr::try_from((
        "b98fb783b49de5652097a989414c767824dff7e7fd765a63b493772511db81c1",
        2,
    ))
    .unwrap();

    assert!(!close_to_chain_head(&block_0, None, offset));
    assert!(!close_to_chain_head(&block_2, None, offset));

    assert!(!close_to_chain_head(
        &block_0,
        Some(block_2.clone()),
        offset
    ));

    assert!(close_to_chain_head(&block_1, Some(block_2.clone()), offset));
    assert!(close_to_chain_head(&block_2, Some(block_2.clone()), offset));
}