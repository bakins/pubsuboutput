# pubsub output plugin for filebeat

This repo provides an output plugin for filebeat that publishes messages to [GCP PubSub](https://cloud.google.com/pubsub)

## Usage

Add the following to your filebeat configuration

```yaml
output.pubsub:
  project_id: my-gcp-project-name
  topic: my-pubsub-topic-name
```

Configuration options:

* project_id: Google Cloud project ID. Required.
* topic: Google Cloud Pub/Sub topic name. Required.
* credentials_file: Path to a JSON file containing the credentials and key used to subscribe. As an alternative you can use the credentials_json config option or rely on [Google Application Default Credentials](https://cloud.google.com/docs/authentication/production).
* credentials_json: JSON blob containing the credentials and key used to subscribe. This can be as an alternative to credentials_file if you want to embed the credential data within your config file or put the information into a keystore. You may also use [Google Application Default Credentials](https://cloud.google.com/docs/authentication/production).

Events are published in gzip compressed json lines format.

## Acknowledgements

Loosely based on https://bionic.fullstory.com/writing-a-filebeat-output-plugin/

## LICENSE

See [LICENSE](./LICENSE)
