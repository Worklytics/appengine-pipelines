# Entity TTLs

The pipelines framework adds TTL fields to all entities it creates, to enable automated cleanup.

By default, the TTLs are set 90 days in the future.

Clean-up will ONLY happen if you add the corresponding TTL policies to your GCP project. See:

https://cloud.google.com/datastore/docs/ttl#create_ttl_policy

You'll have to create ONE ttl policy per entity kind. For clarity, all datastore entities implement the interface `DatastoreEntity`,
so you can search for that in the codebase to find all the entity kinds.

TTL policies apply across ALL namespaces; so if you're using namespaces to segregate pipeline data, you don't need
distinct policies for each namespace.




