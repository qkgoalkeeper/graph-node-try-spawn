use std::sync::Arc;

use async_trait::async_trait;
use graph::blockchain::{Block, Blockchain};
use graph::cheap_clone::CheapClone;
use graph::components::store::SubgraphFork;
use graph::components::subgraph::{MappingError, SharedProofOfIndexing};
use graph::prelude::tokio::time::Instant;
use graph::prelude::{
    BlockState, RuntimeHost, RuntimeHostBuilder, SubgraphInstanceMetrics, TriggerProcessor,
};
use graph::slog::Logger;

pub struct SubgraphTriggerProcessor {}

#[async_trait]
impl<C, T> TriggerProcessor<C, T> for SubgraphTriggerProcessor
where
    C: Blockchain,
    T: RuntimeHostBuilder<C>,
{
    async fn process_trigger(
        &self,
        logger: &Logger,
        hosts: &[Arc<T::Host>],
        block: &Vec<Arc<C::Block>>,
        triggers: &Vec<Vec<C::TriggerData>>,
        mut state: Vec<BlockState<C>>,
        proof_of_indexing: &SharedProofOfIndexing,
        causality_region: &str,
        debug_fork: &Option<Arc<dyn SubgraphFork>>,
        subgraph_metrics: &Arc<SubgraphInstanceMetrics>,
    ) -> Result<Vec<BlockState<C>>, MappingError> {
        let mut error_count = 0;
        for i in 0..state.len(){
            error_count += state[i].deterministic_errors.len();
        }
        

        if let Some(proof_of_indexing) = proof_of_indexing {
            proof_of_indexing
                .borrow_mut()
                .start_handler(causality_region);
        }

        for host in hosts {
            let mut mapping_triggers = Vec::new();
            let mut mapping_trigger_len = 0;
            let mut block_ptrs = Vec::new();

            for i in 0..block.len(){
                block_ptrs.push(block[i].ptr());
                let mut temp_mapping_triggers = Vec::new();

                for trigger in &triggers[i]{
                    match host.match_and_decode(trigger, &block[i], logger)? {
                        // Trigger matches and was decoded as a mapping trigger.
                        Some(mapping_trigger) => temp_mapping_triggers.push(mapping_trigger),
        
                        // Trigger does not match, do not process it.
                        None => continue,
                    };
                }
                mapping_trigger_len+=temp_mapping_triggers.len();
                mapping_triggers.push(temp_mapping_triggers);

            }
            


            if mapping_trigger_len == 0 {
                continue;
            }


            
            

            let start = Instant::now();
            state = host
                .process_mapping_trigger(
                    logger,
                    block_ptrs,
                    mapping_triggers,
                    state,
                    proof_of_indexing.cheap_clone(),
                    debug_fork,
                )
                .await?;
            let elapsed = start.elapsed().as_secs_f64();
            subgraph_metrics.observe_trigger_processing_duration(elapsed);
        }

        // if let Some(proof_of_indexing) = proof_of_indexing {
        //     if state.deterministic_errors.len() != error_count {
        //         // assert!(state.deterministic_errors.len() == error_count + 1);

        //         // If a deterministic error has happened, write a new
        //         // ProofOfIndexingEvent::DeterministicError to the SharedProofOfIndexing.
        //         proof_of_indexing
        //             .borrow_mut()
        //             .write_deterministic_error(&logger, causality_region);
        //     }
        // }

        Ok(state)
    }
}
