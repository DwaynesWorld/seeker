use std::collections::HashMap;
use std::sync::Arc;

use actix_web::{delete, get, post, put, web, HttpResponse, Responder};
use serde::{Deserialize, Serialize};

use crate::clusters::cluster::Cluster;
use crate::clusters::cluster::Kind;
use crate::clusters::store::ClusterStore;

pub fn configure(cfg: &mut web::ServiceConfig) {
    cfg.service(create_cluster)
        .service(get_clusters)
        .service(get_cluster)
        .service(update_cluster)
        .service(delete_cluster);
}

#[post("")]
async fn create_cluster(
    request: web::Json<CreateClusterRequest>,
    store: web::Data<Arc<dyn ClusterStore + Send + Sync>>,
) -> impl Responder {
    info!("creating a new cluster");

    let cluster = Cluster::new(
        request.kind.clone(),
        request.name.clone(),
        request.metadata.clone(),
    );

    match store.insert(cluster).await {
        Ok(id) => HttpResponse::Ok().json(CreateClusterResponse { id }),
        Err(e) => HttpResponse::InternalServerError().body(e.to_string()),
    }
}

#[get("")]
async fn get_clusters() -> impl Responder {
    HttpResponse::Ok().body("Get All")
}

#[get("/{id}")]
async fn get_cluster(id: web::Path<i64>) -> impl Responder {
    HttpResponse::Ok().body(format!("Get: {}", id))
}

#[put("/{id}")]
async fn update_cluster(id: web::Path<i64>) -> impl Responder {
    HttpResponse::Ok().body(format!("Update: {}", id))
}

#[delete("/{id}")]
async fn delete_cluster(id: web::Path<i64>) -> impl Responder {
    HttpResponse::Ok().body(format!("Delete: {}", id))
}

#[derive(Deserialize)]
struct CreateClusterRequest {
    kind: Kind,
    name: String,
    metadata: HashMap<String, String>,
}

#[derive(Serialize)]
struct CreateClusterResponse {
    id: i64,
}
