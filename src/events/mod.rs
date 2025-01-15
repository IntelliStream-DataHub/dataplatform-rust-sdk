use std::rc::{Rc, Weak};
use crate::ApiService;
use crate::http::ResponseError;

pub struct EventsService<'a>{
    pub(crate) api_service: Weak<ApiService<'a>>,
    base_url: String
}

impl<'a> EventsService<'a>{

    pub fn new(api_service: Weak<ApiService<'a>>, base_url: &String) -> Self {
        let base_url = format!("{}/events", base_url);
        EventsService {api_service, base_url}
    }

    fn get_api_service(&self) -> Result<Rc<ApiService<'a>>, ResponseError> {
        self.api_service.upgrade().ok_or_else(|| {
            let err = String::from("Failed to upgrade Weak reference to ApiService");
            eprintln!("{}", err);
            ResponseError::from(err)
        })
    }

}