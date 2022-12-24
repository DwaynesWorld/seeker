use std::collections::HashMap;
use std::sync::Arc;

use actix_web::web::{Data, Json};
use actix_web::{delete, get, post, put, web, HttpResponse, Responder};
use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};

use crate::clusters::cluster::Cluster;
use crate::clusters::cluster::Kind;
use crate::clusters::store::ClusterStore;
use crate::kafka::metadata::service::MetadataService;

pub fn configure(cfg: &mut web::ServiceConfig) {
    cfg.service(create_cluster)
        .service(get_clusters)
        .service(get_cluster)
        .service(update_cluster)
        .service(delete_cluster)
        .service(get_cluster_metadata);
}

#[post("")]
async fn create_cluster(
    r: Json<CreateClusterRequest>,
    store: Data<Arc<dyn ClusterStore + Send + Sync>>,
    metadata_service: Data<MetadataService>,
) -> impl Responder {
    info!("Creating a new cluster");

    let cluster = Cluster::new(None, r.kind.clone(), r.name.clone(), r.config.clone());
    let metadata_service = metadata_service.into_inner().clone();

    match store.insert(&cluster).await {
        Ok(id) => {
            metadata_service.register(Cluster { id, ..cluster }).await;
            HttpResponse::Ok().json(CreateClusterResponse { id })
        }
        Err(e) => HttpResponse::InternalServerError().body(e.to_string()),
    }
}

#[get("")]
async fn get_clusters(store: web::Data<Arc<dyn ClusterStore + Send + Sync>>) -> impl Responder {
    info!("Fetching all clusters");

    match store.list().await {
        Ok(clusters) => {
            let clusters = clusters
                .iter()
                .map(|c| c.to_summary())
                .collect::<Vec<ClusterSummery>>();
            HttpResponse::Ok().json(ListClustersResponse { clusters })
        }
        Err(e) => HttpResponse::InternalServerError().body(e.to_string()),
    }
}

#[get("/{id}")]
async fn get_cluster(
    id: web::Path<i64>,
    store: web::Data<Arc<dyn ClusterStore + Send + Sync>>,
) -> impl Responder {
    let id = id.into_inner();
    info!("Fetching cluster with id {}", id);

    match store.get(id).await {
        Ok(cluster) => {
            let Some(c) = cluster else {
				return HttpResponse::NotFound().finish();
			};

            HttpResponse::Ok().json(ReadClusterResponse {
                cluster: c.to_summary(),
            })
        }
        Err(e) => HttpResponse::InternalServerError().body(e.to_string()),
    }
}

#[put("/{id}")]
async fn update_cluster(
    id: web::Path<i64>,
    r: web::Json<UpdateClusterRequest>,
    store: web::Data<Arc<dyn ClusterStore + Send + Sync>>,
) -> impl Responder {
    let id = id.into_inner();
    info!("Updating cluster with id {}", id);

    let cluster = Cluster::new(Some(id), r.kind.clone(), r.name.clone(), r.config.clone());

    match store.update(&cluster).await {
        Ok(id) => HttpResponse::Ok().json(UpdateClusterResponse { id }),
        Err(e) => HttpResponse::InternalServerError().body(e.to_string()),
    }
}

#[delete("/{id}")]
async fn delete_cluster(
    id: web::Path<i64>,
    store: web::Data<Arc<dyn ClusterStore + Send + Sync>>,
    metadata_service: Data<MetadataService>,
) -> impl Responder {
    let id = id.into_inner();
    info!("Deleting cluster with id {}", id);
    let metadata_service = metadata_service.into_inner().clone();

    match store.remove(id).await {
        Ok(_) => {
            metadata_service.remove(id).await;
            HttpResponse::Ok().finish()
        }
        Err(e) => HttpResponse::InternalServerError().body(e.to_string()),
    }
}

#[get("/{id}/metadata")]
async fn get_cluster_metadata(
    path: web::Path<i64>,
    metadata_service: Data<MetadataService>,
) -> impl Responder {
    let id = path.into_inner();
    info!("Fetching metadata for cluster with id {}", id);

    let result = metadata_service.into_inner().get(id).await;
    if result.is_err() {
        let msg = format!("{}", result.unwrap_err());
        return HttpResponse::InternalServerError().body(msg);
    }

    let entry = result.unwrap();
    if entry.is_none() {
        return HttpResponse::NotFound()
            .body(format!("Cluster metadata with id '{}' not found", id));
    }

    HttpResponse::Ok().json(entry.unwrap())
}

#[derive(Deserialize)]
struct CreateClusterRequest {
    kind: Kind,
    name: String,
    config: HashMap<String, String>,
}

#[derive(Serialize)]
struct CreateClusterResponse {
    id: i64,
}

#[derive(Deserialize)]
struct ListClustersRequest {}

#[derive(Serialize)]
struct ListClustersResponse {
    clusters: Vec<ClusterSummery>,
}

#[derive(Serialize)]
struct ReadClusterResponse {
    cluster: ClusterSummery,
}

#[derive(Deserialize)]
struct UpdateClusterRequest {
    kind: Kind,
    name: String,
    config: HashMap<String, String>,
}

#[derive(Serialize)]
struct UpdateClusterResponse {
    id: i64,
}

#[derive(Serialize)]
struct ClusterSummery {
    id: i64,
    kind: Kind,
    name: String,
    config: HashMap<String, String>,
    created_at: DateTime<Utc>,
    updated_at: DateTime<Utc>,
}

impl Cluster {
    fn to_summary(&self) -> ClusterSummery {
        ClusterSummery {
            id: self.id,
            kind: self.kind.clone(),
            name: self.name.clone(),
            config: self.config.clone(),
            created_at: self.created_at,
            updated_at: self.updated_at,
        }
    }
}
