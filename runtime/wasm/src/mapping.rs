use crate::gas_rules::GasRules;
use crate::module::{ExperimentalFeatures, WasmInstance};
use futures::sync::mpsc;
use futures03::channel::oneshot::Sender;
use graph::blockchain::{Blockchain, HostFn, TriggerWithHandler};
use graph::components::store::SubgraphFork;
use graph::components::subgraph::{MappingError, SharedProofOfIndexing};
use graph::prelude::*;
use graph::runtime::gas::Gas;
use rayon::prelude::*;
// use safina_threadpool::ThreadPool;
use std::collections::BTreeMap;
use std::sync::Arc;
use std::thread;

use threadpool::ThreadPool;
use chrono::prelude::*;
use std::time::{Instant, SystemTime};

/// Spawn a wasm module in its own thread.
pub fn spawn_module<C: Blockchain>(
    raw_module: &[u8],
    logger: Logger,
    subgraph_id: DeploymentHash,
    host_metrics: Arc<HostMetrics>,
    runtime: tokio::runtime::Handle,
    timeout: Option<Duration>,
    experimental_features: ExperimentalFeatures,
) -> Result<mpsc::Sender<MappingRequest<C>>, anyhow::Error> {
    let valid_module = Arc::new(ValidModule::new(&logger, raw_module)?);

    // Create channel for event handling requests
    let (mapping_request_sender, mapping_request_receiver) = mpsc::channel(400);

    // wasmtime instances are not `Send` therefore they cannot be scheduled by
    // the regular tokio executor, so we create a dedicated thread.
    //
    // In case of failure, this thread may panic or simply terminate,
    // dropping the `mapping_request_receiver` which ultimately causes the
    // subgraph to fail the next time it tries to handle an event.

    let conf =
        thread::Builder::new().name(format!("mapping-{}-{}", &subgraph_id, uuid::Uuid::new_v4()));
    conf.spawn(move || {
        let _runtime_guard = runtime.enter();

        // Pass incoming triggers to the WASM module and return entity changes;
        // Stop when canceled because all RuntimeHosts and their senders were dropped.
        mapping_request_receiver
            .map_err(|()| unreachable!())
            .for_each(move |request| {
                let MappingRequest {
                    ctx,
                    triggers,
                    result_sender,
                } = request;

                let init_time = Instant::now();
                let n_workers = 2;
                let n_iter_group = triggers.len() / n_workers;
                let pool = ThreadPool::with_name("mapping_worker".into(), n_workers);

                let (sender, receiver) = std::sync::mpsc::channel();

                let mut trigger_groups = Vec::new();
                let mut trigger_group = Vec::new();
                // println!(
                //     "triggers len: {} n_workers: {} n_iter_group: {} ",
                //     triggers.len(),
                //     n_workers,
                //     n_iter_group
                // );
                for trigger in triggers {
                    trigger_group.push(trigger);
                    if trigger_group.len() == n_iter_group {
                        trigger_groups.push(trigger_group);
                        trigger_group = Vec::new();
                    }
                }

                // 剩余的trigger继续添加到最后一个group
                let tmp = if let Some(mut tmp) = trigger_groups.pop() {
                    tmp.append(&mut trigger_group);
                    trigger_groups.push(tmp);
                };

                // println!("trigger_groups len: {}", trigger_groups.len());
                // println!("init time: {} ms", init_time.elapsed().as_millis());

                for trigger_group in trigger_groups {
                    let t2 = host_metrics.cheap_clone();
                    let t1 = valid_module.cheap_clone();
                    let t3 = timeout.clone();
                    let t4 = experimental_features.clone();
                    let ctx_arc = ctx.derive_with_empty_block_state();
                    let s = sender.clone();
                    pool.execute(move || {
                        let id = thread::current().id();
                        let start_dt = Local::now();
                        let start_time = Instant::now();
                        // println!(
                        //     "thread {:?}  start  time: {} triggers num: {}",
                        //     id,
                        //     start_dt.format("%Y-%m-%d %H:%M:%S:%.3f").to_string(),
                        //     trigger_group.len()
                        // );
                        let result = instantiate_module_and_handle_trigger_group(
                            t1,
                            ctx_arc,
                            trigger_group,
                            t2,
                            t3,
                            t4,
                        );
                        s.send(result).unwrap();
                        // thread::sleep(Duration::from_millis(100));
                        // println!(
                        //     "thread {:?}  used time: {} ms",
                        //     id,
                        //     start_time.elapsed().as_millis()
                        // );
                    });
                }

                let wait_time = Instant::now();
                pool.join();
                // println!("wait join time: {} ms", wait_time.elapsed().as_millis());

                let collect_time = Instant::now();
                let res_groups: Vec<_> = receiver.try_iter().collect();
                let mut res = Vec::new();
                for res_group in res_groups {
                    for res_temp in res_group {
                        res.push(res_temp);
                    }
                }
                // println!("collect  time: {} ms", collect_time.elapsed().as_millis());

                let send_time = Instant::now();
                result_sender.send(res).unwrap();
                // println!("send  time: {} ms", send_time.elapsed().as_millis());

                Ok(())
            })
            .wait()
            .unwrap();
        // .wait()
    })
    .map(|_| ())
    .context("Spawning WASM runtime thread failed")?;

    Ok(mapping_request_sender)
}

fn instantiate_module_and_handle_trigger_group<C: Blockchain>(
    valid_module: Arc<ValidModule>,
    ctx: MappingContext<C>,
    trigger_group: Vec<TriggerWithHandler<C>>,
    host_metrics: Arc<HostMetrics>,
    timeout: Option<Duration>,
    experimental_features: ExperimentalFeatures,
) -> Vec<Result<(BlockState<C>, Gas), MappingError>> {
    let mut res: Vec<Result<(BlockState<C>, Gas), MappingError>> = Vec::new();

    for trigger in trigger_group {
        let c_valid_module = valid_module.clone();
        let c_ctx = ctx.derive_with_empty_block_state();
        let c_host_metrics = host_metrics.clone();
        let c_timeout = timeout.clone();
        let c_experimental_features = experimental_features.clone();
        let logger = c_ctx.logger.cheap_clone();
        // Start the WASM module runtime.

        // let section = host_metrics.stopwatch.start_section("module_init");
        let module = WasmInstance::from_valid_module_with_ctx(
            c_valid_module,
            c_ctx,
            c_host_metrics,
            c_timeout,
            c_experimental_features,
        );
        // section.end();

        // let _section = host_metrics.stopwatch.start_section("run_handler");
        if ENV_VARS.log_trigger_data {
            debug!(logger, "trigger data: {:?}", trigger);
        }
        let result = module.expect("REASON").handle_trigger(trigger);
        res.push(result);
    }
    res
}

pub struct MappingRequest<C: Blockchain> {
    pub(crate) ctx: MappingContext<C>,
    pub(crate) triggers: Vec<TriggerWithHandler<C>>,
    pub(crate) result_sender: Sender<Vec<Result<(BlockState<C>, Gas), MappingError>>>,
}

#[derive(Clone)]
pub struct MappingContext<C: Blockchain> {
    pub logger: Logger,
    pub host_exports: Arc<crate::host_exports::HostExports<C>>,
    pub block_ptr: BlockPtr,
    pub state: BlockState<C>,
    pub proof_of_indexing: SharedProofOfIndexing,
    pub host_fns: Arc<Vec<HostFn>>,
    pub debug_fork: Option<Arc<dyn SubgraphFork>>,
}

impl<C: Blockchain> MappingContext<C> {
    pub fn derive_with_empty_block_state(&self) -> Self {
        MappingContext {
            logger: self.logger.cheap_clone(),
            host_exports: self.host_exports.cheap_clone(),
            block_ptr: self.block_ptr.cheap_clone(),
            state: BlockState::new(self.state.entity_cache.store.clone(), Default::default()),
            proof_of_indexing: self.proof_of_indexing.cheap_clone(),
            host_fns: self.host_fns.cheap_clone(),
            debug_fork: self.debug_fork.cheap_clone(),
        }
    }
}

/// A pre-processed and valid WASM module, ready to be started as a WasmModule.
pub struct ValidModule {
    pub module: wasmtime::Module,

    // A wasm import consists of a `module` and a `name`. AS will generate imports such that they
    // have `module` set to the name of the file it is imported from and `name` set to the imported
    // function name or `namespace.function` if inside a namespace. We'd rather not specify names of
    // source files, so we consider that the import `name` uniquely identifies an import. Still we
    // need to know the `module` to properly link it, so here we map import names to modules.
    //
    // AS now has an `@external("module", "name")` decorator which would make things cleaner, but
    // the ship has sailed.
    pub import_name_to_modules: BTreeMap<String, Vec<String>>,
}

impl ValidModule {
    /// Pre-process and validate the module.
    pub fn new(logger: &Logger, raw_module: &[u8]) -> Result<Self, anyhow::Error> {
        // Add the gas calls here. Module name "gas" must match. See also
        // e3f03e62-40e4-4f8c-b4a1-d0375cca0b76. We do this by round-tripping the module through
        // parity - injecting gas then serializing again.
        let parity_module = parity_wasm::elements::Module::from_bytes(raw_module)?;
        let parity_module = match parity_module.parse_names() {
            Ok(module) => module,
            Err((errs, module)) => {
                for (index, err) in errs {
                    warn!(
                        logger,
                        "unable to parse function name for index {}: {}",
                        index,
                        err.to_string()
                    );
                }

                module
            }
        };
        let parity_module = wasm_instrument::gas_metering::inject(parity_module, &GasRules, "gas")
            .map_err(|_| anyhow!("Failed to inject gas counter"))?;
        let raw_module = parity_module.into_bytes()?;

        // We currently use Cranelift as a compilation engine. Cranelift is an optimizing compiler,
        // but that should not cause determinism issues since it adheres to the Wasm spec. Still we
        // turn off optional optimizations to be conservative.
        let mut config = wasmtime::Config::new();
        config.strategy(wasmtime::Strategy::Cranelift).unwrap();
        config.interruptable(true); // For timeouts.
        config.cranelift_nan_canonicalization(true); // For NaN determinism.
        config.cranelift_opt_level(wasmtime::OptLevel::None);
        config
            .max_wasm_stack(ENV_VARS.mappings.max_stack_size)
            .unwrap(); // Safe because this only panics if size passed is 0.

        let engine = &wasmtime::Engine::new(&config)?;
        let module = wasmtime::Module::from_binary(&engine, &raw_module)?;

        let mut import_name_to_modules: BTreeMap<String, Vec<String>> = BTreeMap::new();

        // Unwrap: Module linking is disabled.
        for (name, module) in module
            .imports()
            .map(|import| (import.name().unwrap(), import.module()))
        {
            import_name_to_modules
                .entry(name.to_string())
                .or_default()
                .push(module.to_string());
        }

        Ok(ValidModule {
            module,
            import_name_to_modules,
        })
    }
}
