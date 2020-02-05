const { Transformer, gql } = require("graphql-transformer-core");

const TABLE_RESOURCE = 'AWS::DynamoDB::Table';
const DDB_CREATION_CONCURRENCY_LIMIT = 10;

const partitionCollection = (collection, size = DDB_CREATION_CONCURRENCY_LIMIT) => {
  let worker = [];
  let withinPartition = false;
  const partitioned = collection.reduce((acc, data, i) => {
    withinPartition = true;
    worker.push(data);
    if (i > 0 && i % size === 0) {
      withinPartition = false
      acc.push(worker);
      worker = [];
    }
    return acc;
  }, []);

  if (withinPartition && worker.length) {
    partitioned.push(worker);
  }
  return partitioned;
}

class CapbaseDynamoDBModelDependsOnTransformer extends Transformer {
  /**
   * There isn't really a directive associated with this transformer,
   * but `Transformer` needs at least a dummy one passed in for initialization.
   */
  constructor() {
    super(
      'CapbaseDynamoDBModelDependsOnTransformer',
      gql`directive @capbaseDDBModelDependencies on OBJECT`
    );
  }

  /**
   * required by `Transformer` class to have a definition here so we just define a noop
   */
  object = (def, directive, ctx) => {/* noop */}

  /**
   * After the `TransformationContext` is fully built up, go through each table definition
   * and add to its DependsOn property some other table definitions so that the concurrent
   * creation limit of ddb tables is not reached.
   */
  after = (ctx) => {
    super.after && super.after(ctx);
    const template = ctx.template;

    const tableResourceData = Object.keys(template.Resources).filter((resourceId) => {
      const resource = ctx.getResource(resourceId);
      return resource.Type === TABLE_RESOURCE;
    }).map((resourceId) => {
      return {
        resourceId,
        resource: ctx.getResource(resourceId),
      }
    });

    const partitioned = partitionCollection(tableResourceData);

    partitioned.forEach((partition, i) => {
      partition.forEach(({ resourceId, resource }, j) => {
        const DependsOn = (resource.Properties.DependsOn || []).slice();

        partitioned.forEach((innerPartition, k) => {
          if (i !== k) {
            const otherPartitionData = innerPartition[j];
            if (otherPartitionData && !DependsOn.includes(otherPartitionData.resourceId)) {
              DependsOn.push(otherPartitionData.resourceId)
            }
          }
        });

        resource.Properties.DependsOn = DependsOn;
      })
    });
  }
}

module.exports = { default: CapbaseDynamoDBModelDependsOnTransformer };
