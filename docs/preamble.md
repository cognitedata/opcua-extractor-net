To configure the OPC UA extractor, you must edit the configuration file. The file is in [YAML](https://yaml.org/) format, and the sample configuration file contains all valid options with default values.

You can leave many fields empty to let the extractor use the default values. The configuration file separates the settings by component, and you can remove an entire component to disable it or use the default values.

## Sample configuration files

In the extractor installation folder, the `/config` subfolder contains sample complete and minimal configuration files. The values wrapped in `${}` are replaced with environment variables with that name. For example `${COGNITE_PROJECT}` will be replaced with the value of the environment variable called `COGNITE_PROJECT`.

The configuration file also contains the global parameter `version`, which holds the version of the configuration schema used in the configuration file. This document describes version 1 of the configuration schema.

Not that it is _not_ recommended to use the `config.example.yml` as a basis for configuration files. This file contains _all_ configuration options, which is both hard to read, and may cause issues. It is intended as a reference showing how each option is configured, _not_ as a basis. Use `config.minimal.yml` instead.

:::tip Tip
You can set up [extraction pipelines](../../interfaces/configure_integrations.md) to use versioned extractor configuration files stored in the cloud.
:::

## Minimal YAML configuration file


```yml showLineNumbers
version: 1

source:
  # The URL of the OPC-UA server to connect to
  endpoint-url: 'opc.tcp://localhost:4840'

cognite:
  # The project to connect to in the API, uses the environment variable COGNITE_PROJECT.
  project: '${COGNITE_PROJECT}'
  # Cognite authentication
  # This is for Microsoft as IdP. To use a different provider,
  # set implementation: Basic, and use token-url instead of tenant.
  # See the example config for the full list of options.
  idp-authentication:
    # Directory tenant
    tenant: ${COGNITE_TENANT_ID}
    # Application Id
    client-id: ${COGNITE_CLIENT_ID}
    # Client secret
    secret: ${COGNITE_CLIENT_SECRET}
    # List of resource scopes, ex:
    # scopes:
    #   - scopeA
    #   - scopeB
    scopes:
      - ${COGNITE_SCOPE}

extraction:
  # Global prefix for externalId in destinations. Should be unique to prevent name conflicts.
  id-prefix: 'gp:'
  # Map OPC-UA namespaces to prefixes in CDF. If not mapped, the full namespace URI is used.
  # Saves space compared to using the full URL. Using the ns index is not safe as the order can change on the server.
  # It is recommended to set this before extracting the node hierarchy.
  # For example:
  # NamespaceMap:
  #   "urn:cognite:net:server": cns
  #   "urn:freeopcua:python:server": fps
  #   "http://examples.freeopcua.github.io": efg
```

## ProtoNodeId

You can provide an OPC UA `NodeId` in several places in the configuration file, these are YAML objects with the following structure:

```yaml
node:
    node-id: i=123
    namespace-uri: opc.tcp://test.test
```

To find the node IDs we recommend using the [UAExpert](https://www.unified-automation.com/products/development-tools/uaexpert.html) tool.

Locate the node you need the ID of in the hierarchy, the find the node ID on the right side under **Attribute** > **NodeId**. Find the **Namespace Uri** by matching the **NamespaceIndex** on the right to the dropdown on the left, in the node hierarchy view. The default value is **No highlight**.

## <a name="timestamps-and-intervals"></a>Timestamps and intervals

In most places where time intervals are required, you can use a CDF-like syntax of `[N][timeunit]`, for example `10m` for 10 minutes or `1h` for 1 hour. `timeunit` is one of `d`, `h`, `m`, `s`, `ms`. You can also use a cron expression in some places.

For history start and end times you can use a similar syntax. `[N][timeunit]` and `[N][timeunit]-ago`. `1d-ago` means 1 day in the past from the time history starts, and `1h` means 1 hour in the future. For instance, you can use this syntax to configure the extractor to read only recent history.

