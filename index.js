const { Transformer, gql, TRANSFORM_CONFIG_FILE_NAME } = require("graphql-transformer-core");
const { Context } = require('@aws-amplify/cli/lib/domain/context');
const fs = require('fs');
const path = require('path');

const TABLE_RESOURCE_TYPE = 'AWS::DynamoDB::Table'
const DDB_CREATION_CONCURRENCY_LIMIT = 10;
const ROOT_STACK_ID = 'root';
const BACKEND_CATEGORY = 'api';

/**
 * Finds the full path for a file if it exists under `basePath`.
 * Looks recursively.
 */
const findFullPathForFileSync = (basePath, fileName) => {
  const names = fs.readdirSync(basePath);
  let filePaths = [];
  for (let i = 0; i < names.length; i++) {
    const name = names[i];
    const newPath = path.join(basePath, name);
    const pathStat = fs.statSync(newPath);

    filePaths.push(newPath);

    if (pathStat.isFile() && name === fileName) {
      break;
    }

    if (pathStat.isDirectory()) {
      try {
        filePaths = filePaths.concat(findFullPathForFileSync(newPath, fileName));
      } catch (ex) {
        continue;
      }
    }
  }

  return filePaths.filter((path) => {
    return path.endsWith(fileName);
  });
}

/**
 * Load `transform.conf.json` configuration file
 */
const loadConfig = () => {
  let configPath = '';
  try {
    const context = new Context();
    configPath = findFullPathForFileSync(
      `${context.amplify.pathManager.getBackendDirPath(process.cwd())}/${BACKEND_CATEGORY}`,
      TRANSFORM_CONFIG_FILE_NAME);

    if (configPath && Array.isArray(configPath)) {
      configPath = configPath[0];
    }

    const serializedConfig = fs.readFileSync(configPath);
    const config = JSON.parse(serializedConfig.toString());
    return config;
  } catch (err) {
    console.log(`WARNING: AmplifyCliDynamoDBModelDependenciesTransformer could not read transform configuration at ${configPath}`);
  }
}

/**
 * A comparator used for sorting a flat collection of
 * resource data objects
 */
const comparatorFactory = (stackMapping) => {
  return (a, b) => {
    const aStackId = stackMapping.get(a.resourceId);
    const bStackId = stackMapping.get(b.resourceId);

    if (aStackId === ROOT_STACK_ID && bStackId !== ROOT_STACK_ID) {
      return -1;
    }
    if (aStackId !== ROOT_STACK_ID && bStackId === ROOT_STACK_ID) {
      return 1;
    }

    if (aStackId > bStackId) {
      return 1;
    }
    if (aStackId < bStackId) {
      return -1;
    }
    return 0;
  };
};

/**
 * Injects a DependsOn property
 */
const injectDependsOnDependency = (resource, dependencyResourceId) => {
  resource.DependsOn = resource.DependsOn || [];
  resource.DependsOn.push(dependencyResourceId);
}

/**
 * Injects the dependency tag reference into the resource properties
 */
const injectTagReferenceDependency = (stackMapping, resource, resourceId, dependencyResourceId) => {
  let stackId = stackMapping.get(resourceId);
  stackId = `${stackId.charAt(0).toUpperCase()}${stackId.slice(1)}`;

  resource.Properties = resource.Properties || {};
  resource.Properties.Tags = resource.Properties.Tags || [];
  resource.Properties.Tags.push({
    Key: `Injected${stackId}${dependencyResourceId}Dependency`,
    Value: {
      Ref: dependencyResourceId,
    }
  });
}

/**
 * Partitions a flat collection up into n partitions of m size
 */
const partitionCollection = (collection, size = DDB_CREATION_CONCURRENCY_LIMIT) => {
  let worker = [];
  let withinPartition = false;

  const partitioned = collection.reduce((acc, data, i) => {
    withinPartition = true;
    worker.push(data);
    if ((i + 1) % size === 0) {
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
};

/**
 * A custom `amplify cli` transformer that creates dependencies across
 * table definitions to allow only 10 at a time to concurrently create
 * which is the current aws limit.
 */
class AmplifyCliDynamoDBModelDependenciesTransformer extends Transformer {
  /**
   * There isn't really a directive associated with this transformer,
   * but `Transformer` needs at least a dummy one passed in for proper
   * initialization.
   */
  constructor() {
    super(
      'AmplifyCliDynamoDBModelDependenciesTransformer',
      gql`directive @amplifyCliDDBModelDependencies on OBJECT`
    );
  }

  /**
   * Required by `Transformer` class to have a definition here because of where we
   * arbitrarily chose to bind our directive in the schema just above, so we just
   * define this as a noop
   */
  object = (def, directive, ctx) => {/* noop */}

  /**
   * After the `TransformationContext` is fully built up, go through each table definition
   * and add properties importing some other table definitions into the stack with the goal of
   * reducing the overall number of concurrent table creations so that the concurrent creation
   * limit of ddb tables is not reached.
   */
  after = (ctx) => {
    const template = ctx.template;

    // Grab migration resource info from `transform.conf.json` which unfortunately is not
    // directly available in the transformers
    debugger;
    const transformConfig = loadConfig();
    const stackMappingIncludingHoisted = new Map(ctx.getStackMapping());
    if (
      transformConfig &&
      transformConfig.Migration &&
      transformConfig.Migration.V1 &&
      transformConfig.Migration.V1.Resources
    ) {
      transformConfig.Migration.V1.Resources.forEach((resourceId) => {
        stackMappingIncludingHoisted.set(resourceId, ROOT_STACK_ID);
      });
    }

    const tableResourceData = Object.keys(template.Resources)
      .filter((resourceId) => {
        const resource = ctx.getResource(resourceId);
        return resource.Type === TABLE_RESOURCE_TYPE;
      }).map((resourceId) => {
        return {
          resourceId,
          resource: ctx.getResource(resourceId),
        }
      }).sort(comparatorFactory(stackMappingIncludingHoisted));

    // First process any tables defined in the root stack (usually due to migration
    // from older cli). These should only reference each other.
    const rootResources = tableResourceData.filter(({ resourceId }) => {
      return stackMappingIncludingHoisted.get(resourceId) === ROOT_STACK_ID;
    });

    rootResources.slice(1).forEach(({ resource }, i) => {
      injectDependsOnDependency(
        resource, rootResources[i].resourceId);
    });

    // Now process all tables defined in nested stacks
    const nestedResources = tableResourceData.filter(({ resourceId }) => {
      return stackMappingIncludingHoisted.get(resourceId) !== ROOT_STACK_ID;
    });

    // Partition the nested stacks into groups of 10
    const partitioned = partitionCollection(nestedResources);

    /**
     * Rules for adding nested stack dependencies:
     * 1.) We don't want any from the first partition to depend on any other tables
     * 2.) Each table in the partitions after the first should depend on the resource
     *     at the same position within the previous partition.
     */
    partitioned.slice(1).forEach((partition, i) => {
      partition.forEach(({ resourceId, resource }, j) => {
        const dependencyPartition = partitioned[i];
        const { resourceId: dependencyResourceId } = (dependencyPartition[j] || {});

        if (dependencyResourceId) {
          injectTagReferenceDependency(
            stackMappingIncludingHoisted, resource, resourceId, dependencyResourceId);
        }
      });
    });
  }
}

module.exports = { default: AmplifyCliDynamoDBModelDependenciesTransformer };
