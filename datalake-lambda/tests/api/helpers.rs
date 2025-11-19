use reqwest::Client as ReqClient;
use reqwest::Response;

pub struct TestApp {
    pub address: String,
    pub http_client: ReqClient,
}

impl TestApp {
    pub fn new(address: String) -> Self {
        let http_client = ReqClient::builder().build().unwrap();

        Self {
            address,
            http_client,
        }
    }

    // pub async fn get_alive<Body>(&self, body: &Body) -> Response
    // where
    //     Body: serde::Serialize,
    // {
    //     self.http_client
    //         .get(&format!("{}/alive", &self.address))
    //         .json(body)
    //         .send()
    //         .await
    //         .expect("Failed to execute request.")
    // }

    pub async fn post_query<Body>(&self, body: &Body) -> Response
    where
        Body: serde::Serialize,
    {
        self.http_client
            .post(&format!("{}/query", &self.address))
            .json(body)
            .send()
            .await
            .expect("Failed to execute request.")
    }
}
