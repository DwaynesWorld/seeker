# Seekr

A powerful and flexible user interface for managing Apache Kafka. Handle your day-to-day tasks with ease, find exactly what you're looking for, and fix issues quickly.

## Endpoints

### Cluster Configuration
The endpoints create, update, delete and query cluster configurations registered with Seeker

- List Clusters: `GET api/v1/clusters/:kind`
- Get Cluster: `GET api/v1/clusters/:id`
- Create Cluster:  `POST api/v1/clusters`
- Update Cluster:  `PUT api/v1/clusters/:id`
- Delete Cluster: `DELETE api/v1/clusters/:id`


### Subscriptions
The endpoints create, update, delete and query provide configuration for topic subscriptions

- List Subscriptions: `GET api/v1/subscriptions`
- Get Subscription: `GET api/v1/subscriptions/:id`
- Create Subscription:  `POST api/v1/subscriptions`
- Update Subscription:  `PUT api/v1/subscriptions/:id`
- Delete Subscription: `DELETE api/v1/subscriptions/:id`
