use std::collections::HashMap;
use std::sync::Arc;

use actix_web::{delete, get, post, put, web, HttpResponse, Responder};
use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};

use crate::clusters::store::ClusterStore;
use crate::errors::AnyError;
use crate::subscriptions::store::SubscriptionStore;
use crate::subscriptions::subscription::Subscription;

pub fn configure(cfg: &mut web::ServiceConfig) {
    cfg.service(create_subscription)
        .service(get_subscriptions)
        .service(get_subscription)
        .service(update_subscription)
        .service(delete_subscription);
}

#[post("")]
async fn create_subscription(
    r: web::Json<CreateSubscriptionRequest>,
    cs: web::Data<Arc<dyn ClusterStore + Send + Sync>>,
    ss: web::Data<Arc<dyn SubscriptionStore + Send + Sync>>,
) -> impl Responder {
    info!("Creating a new subscription");

    let result = cluster_exist(r.cluster_id, cs).await;
    if result.is_err() {
        return HttpResponse::InternalServerError().body(result.unwrap_err().to_string());
    }

    if result.unwrap() == false {
        return HttpResponse::NotFound()
            .body(format!("Cluster with id '{}' not found", r.cluster_id));
    }

    let subscription =
        Subscription::new(None, r.cluster_id, r.topic_name.clone(), r.config.clone());

    match ss.insert(subscription).await {
        Ok(id) => HttpResponse::Ok().json(CreateSubscriptionResponse { id }),
        Err(e) => HttpResponse::InternalServerError().body(e.to_string()),
    }
}

#[get("/{cluster_id}")]
async fn get_subscriptions(
    path: web::Path<i64>,
    cs: web::Data<Arc<dyn ClusterStore + Send + Sync>>,
    ss: web::Data<Arc<dyn SubscriptionStore + Send + Sync>>,
) -> impl Responder {
    let cluster_id = path.into_inner();
    info!(
        "Listing all subscriptions in cluster with id {}",
        cluster_id
    );

    let result = cluster_exist(cluster_id, cs).await;
    if result.is_err() {
        return HttpResponse::InternalServerError().body(result.unwrap_err().to_string());
    }

    if result.unwrap() == false {
        return HttpResponse::NotFound()
            .body(format!("Cluster with id '{}' not found", cluster_id));
    }

    match ss.list(Some(cluster_id)).await {
        Ok(subscriptions) => {
            let subscriptions = subscriptions
                .iter()
                .map(|c| c.to_summary())
                .collect::<Vec<SubscriptionSummery>>();
            HttpResponse::Ok().json(ListSubscriptionsResponse { subscriptions })
        }
        Err(e) => HttpResponse::InternalServerError().body(e.to_string()),
    }
}

#[get("/{cluster_id}/{id}")]
async fn get_subscription(
    path: web::Path<(i64, i64)>,
    cs: web::Data<Arc<dyn ClusterStore + Send + Sync>>,
    ss: web::Data<Arc<dyn SubscriptionStore + Send + Sync>>,
) -> impl Responder {
    let (cluster_id, id) = path.into_inner();
    info!(
        "Fetching subscription from cluster id {} with id {}",
        cluster_id, id
    );

    let result = cluster_exist(cluster_id, cs).await;
    if result.is_err() {
        return HttpResponse::InternalServerError().body(result.unwrap_err().to_string());
    }

    if result.unwrap() == false {
        return HttpResponse::NotFound()
            .body(format!("Cluster with id '{}' not found", cluster_id));
    }

    match ss.get(cluster_id, id).await {
        Ok(subscription) => {
            let Some(s) = subscription else {
				return HttpResponse::NotFound().finish();
			};

            HttpResponse::Ok().json(ReadSubscriptionResponse {
                subscription: s.to_summary(),
            })
        }
        Err(e) => HttpResponse::InternalServerError().body(e.to_string()),
    }
}

#[put("/{cluster_id}/{id}")]
async fn update_subscription(
    path: web::Path<(i64, i64)>,
    r: web::Json<UpdateSubscriptionRequest>,
    cs: web::Data<Arc<dyn ClusterStore + Send + Sync>>,
    ss: web::Data<Arc<dyn SubscriptionStore + Send + Sync>>,
) -> impl Responder {
    let (cluster_id, id) = path.into_inner();
    info!(
        "Updating subscription from cluster id {} with id {}",
        cluster_id, id
    );

    let result = cluster_exist(cluster_id, cs).await;
    if result.is_err() {
        return HttpResponse::InternalServerError().body(result.unwrap_err().to_string());
    }

    if result.unwrap() == false {
        return HttpResponse::NotFound()
            .body(format!("Cluster with id '{}' not found", cluster_id));
    }

    let subscription =
        Subscription::new(Some(id), cluster_id, r.topic_name.clone(), r.config.clone());

    match ss.update(subscription).await {
        Ok(id) => HttpResponse::Ok().json(UpdateSubscriptionResponse { id }),
        Err(e) => HttpResponse::InternalServerError().body(e.to_string()),
    }
}

#[delete("/{cluster_id}/{id}")]
async fn delete_subscription(
    path: web::Path<(i64, i64)>,
    ss: web::Data<Arc<dyn SubscriptionStore + Send + Sync>>,
) -> impl Responder {
    let (cluster_id, id) = path.into_inner();
    info!(
        "Deleting subscription from cluster id {} with id {}",
        cluster_id, id
    );

    match ss.remove(cluster_id, id).await {
        Ok(_) => HttpResponse::Ok().finish(),
        Err(e) => HttpResponse::InternalServerError().body(e.to_string()),
    }
}

async fn cluster_exist(
    cluster_id: i64,
    cs: web::Data<Arc<dyn ClusterStore + Send + Sync>>,
) -> Result<bool, AnyError> {
    let cluster = cs.get(cluster_id).await?;
    Ok(cluster.is_some())
}

#[derive(Deserialize)]
struct CreateSubscriptionRequest {
    cluster_id: i64,
    topic_name: String,
    config: HashMap<String, String>,
}

#[derive(Serialize)]
struct CreateSubscriptionResponse {
    id: i64,
}

#[derive(Serialize)]
struct ListSubscriptionsResponse {
    subscriptions: Vec<SubscriptionSummery>,
}

#[derive(Serialize)]
struct ReadSubscriptionResponse {
    subscription: SubscriptionSummery,
}

#[derive(Deserialize)]
struct UpdateSubscriptionRequest {
    topic_name: String,
    config: HashMap<String, String>,
}

#[derive(Serialize)]
struct UpdateSubscriptionResponse {
    id: i64,
}

#[derive(Serialize)]
struct SubscriptionSummery {
    id: i64,
    cluster_id: i64,
    topic_name: String,
    config: HashMap<String, String>,
    created_at: DateTime<Utc>,
    updated_at: DateTime<Utc>,
}

impl Subscription {
    fn to_summary(&self) -> SubscriptionSummery {
        SubscriptionSummery {
            id: self.id,
            cluster_id: self.cluster_id,
            topic_name: self.topic_name.clone(),
            config: self.config.clone(),
            created_at: self.created_at,
            updated_at: self.updated_at,
        }
    }
}
