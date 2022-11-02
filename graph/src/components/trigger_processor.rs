use std::sync::Arc;

use async_trait::async_trait;
use slog::Logger;

use crate::{blockchain::Blockchain, prelude::SubgraphInstanceMetrics};

use super::{
    store::SubgraphFork,
    subgraph::{BlockState, MappingError, RuntimeHostBuilder, SharedProofOfIndexing},
};

#[async_trait]
pub trait TriggerProcessor<C, T>: Sync + Send
where
    C: Blockchain,
    T: RuntimeHostBuilder<C>,
{
    async fn process_trigger(
        &self,
        logger: &Logger,
        hosts: &[Arc<T::Host>],
        block: &Vec<Arc<C::Block>>,
        trigger: &Vec<Vec<C::TriggerData>>,
        mut state: Vec<BlockState<C>>,
        proof_of_indexing: &SharedProofOfIndexing,
        causality_region: &str,
        debug_fork: &Option<Arc<dyn SubgraphFork>>,
        subgraph_metrics: &Arc<SubgraphInstanceMetrics>,
    ) -> Result<Vec<BlockState<C>>, MappingError>;

}
