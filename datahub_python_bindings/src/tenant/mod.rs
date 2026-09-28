use intellistream_datahub_sdk::tenant::{
    SettingsPermission, TenantFeatures, TenantLlmSettings, TenantLlmSettingsForm,
};
use intellistream_datahub_sdk::ApiService;
use pyo3::prelude::*;
use pyo3_async_runtimes::tokio::future_into_py;
use std::collections::HashMap;
use std::sync::Arc;

/// Which optional features are enabled for your tenant.
#[pyclass(module = "intellistream_datahub_sdk", name = "TenantFeatures", get_all, frozen, skip_from_py_object)]
#[derive(Clone)]
pub struct PyTenantFeatures {
    pub files: bool,
    pub policy: bool,
    pub streaming: bool,
    pub chat: bool,
}

impl From<TenantFeatures> for PyTenantFeatures {
    fn from(f: TenantFeatures) -> Self {
        Self {
            files: f.files,
            policy: f.policy,
            streaming: f.streaming,
            chat: f.chat,
        }
    }
}

#[pymethods]
impl PyTenantFeatures {
    fn __repr__(&self) -> String {
        let b = |v: bool| if v { "True" } else { "False" };
        format!(
            "TenantFeatures(files={}, policy={}, streaming={}, chat={})",
            b(self.files),
            b(self.policy),
            b(self.streaming),
            b(self.chat)
        )
    }
}

/// What the caller may do with one settings scope.
#[pyclass(module = "intellistream_datahub_sdk", name = "SettingsPermission", get_all, frozen, skip_from_py_object)]
#[derive(Clone)]
pub struct PySettingsPermission {
    pub read: bool,
    pub write: bool,
}

impl From<SettingsPermission> for PySettingsPermission {
    fn from(p: SettingsPermission) -> Self {
        Self {
            read: p.read,
            write: p.write,
        }
    }
}

#[pymethods]
impl PySettingsPermission {
    fn __repr__(&self) -> String {
        let b = |v: bool| if v { "True" } else { "False" };
        format!("SettingsPermission(read={}, write={})", b(self.read), b(self.write))
    }
}

/// The model your organization's assistant runs on. The API key is never returned; `api_key_set`
/// says whether one is stored.
#[pyclass(module = "intellistream_datahub_sdk", name = "TenantLlmSettings", get_all, frozen, skip_from_py_object)]
#[derive(Clone)]
pub struct PyTenantLlmSettings {
    pub provider: Option<String>,
    pub model: Option<String>,
    pub base_url: Option<String>,
    pub reasoning_effort: Option<String>,
    pub effort: Option<String>,
    pub turn_timeout: Option<String>,
    pub max_output_tokens: Option<u32>,
    pub max_iterations: Option<u32>,
    pub instructions: Option<String>,
    pub api_key_set: bool,
    /// Whether this amounts to a model that can actually be called.
    pub configured: bool,
}

impl From<TenantLlmSettings> for PyTenantLlmSettings {
    fn from(s: TenantLlmSettings) -> Self {
        Self {
            provider: s.provider,
            model: s.model,
            base_url: s.base_url,
            reasoning_effort: s.reasoning_effort,
            effort: s.effort,
            turn_timeout: s.turn_timeout,
            max_output_tokens: s.max_output_tokens,
            max_iterations: s.max_iterations,
            instructions: s.instructions,
            api_key_set: s.api_key_set,
            configured: s.configured,
        }
    }
}

#[pymethods]
impl PyTenantLlmSettings {
    fn __repr__(&self) -> String {
        format!(
            "TenantLlmSettings(provider={:?}, model={:?}, configured={})",
            self.provider,
            self.model,
            if self.configured { "True" } else { "False" }
        )
    }
}

fn permissions_to_py(map: HashMap<String, SettingsPermission>) -> HashMap<String, PySettingsPermission> {
    map.into_iter().map(|(k, v)| (k, v.into())).collect()
}

#[allow(clippy::too_many_arguments)]
fn llm_form(
    provider: Option<String>,
    model: Option<String>,
    api_key: Option<String>,
    base_url: Option<String>,
    reasoning_effort: Option<String>,
    effort: Option<String>,
    turn_timeout: Option<String>,
    max_output_tokens: Option<u32>,
    max_iterations: Option<u32>,
    instructions: Option<String>,
) -> TenantLlmSettingsForm {
    TenantLlmSettingsForm {
        provider,
        model,
        api_key,
        base_url,
        reasoning_effort,
        effort,
        turn_timeout,
        max_output_tokens,
        max_iterations,
        instructions,
    }
}

#[pyclass(module = "intellistream_datahub_sdk", name = "TenantServiceSync")]
pub struct PyTenantServiceSync {
    pub api_service: Arc<ApiService>,
    pub runtime: Arc<tokio::runtime::Runtime>,
}

#[pymethods]
impl PyTenantServiceSync {
    /// Which optional features are enabled for your tenant.
    fn features(&self, py: Python<'_>) -> PyResult<PyTenantFeatures> {
        let service = self.api_service.clone();
        py.detach(|| {
            self.runtime
                .block_on(service.tenant.features())
                .map(Into::into)
                .map_err(crate::datahub_err)
        })
    }

    /// What you may read and write, per settings scope (`"llm"`, …). For gating a UI; the settings
    /// calls enforce the same grants.
    fn settings_permissions(&self, py: Python<'_>) -> PyResult<HashMap<String, PySettingsPermission>> {
        let service = self.api_service.clone();
        py.detach(|| {
            self.runtime
                .block_on(service.tenant.settings_permissions())
                .map(permissions_to_py)
                .map_err(crate::datahub_err)
        })
    }

    /// Your organization's model configuration. Needs the `llm` read grant.
    fn llm_settings(&self, py: Python<'_>) -> PyResult<PyTenantLlmSettings> {
        let service = self.api_service.clone();
        py.detach(|| {
            self.runtime
                .block_on(service.tenant.llm_settings())
                .map(Into::into)
                .map_err(crate::datahub_err)
        })
    }

    /// **Replace** the model configuration: an argument left out is cleared, except `api_key`,
    /// where `None` or empty keeps the stored credential. Needs the `llm` write grant.
    #[pyo3(signature = (provider=None, model=None, api_key=None, base_url=None,
                        reasoning_effort=None, effort=None, turn_timeout=None,
                        max_output_tokens=None, max_iterations=None, instructions=None))]
    #[allow(clippy::too_many_arguments)]
    fn update_llm_settings(
        &self,
        py: Python<'_>,
        provider: Option<String>,
        model: Option<String>,
        api_key: Option<String>,
        base_url: Option<String>,
        reasoning_effort: Option<String>,
        effort: Option<String>,
        turn_timeout: Option<String>,
        max_output_tokens: Option<u32>,
        max_iterations: Option<u32>,
        instructions: Option<String>,
    ) -> PyResult<PyTenantLlmSettings> {
        let form = llm_form(
            provider, model, api_key, base_url, reasoning_effort, effort, turn_timeout,
            max_output_tokens, max_iterations, instructions,
        );
        let service = self.api_service.clone();
        py.detach(|| {
            self.runtime
                .block_on(service.tenant.update_llm_settings(&form))
                .map(Into::into)
                .map_err(crate::datahub_err)
        })
    }
}

#[pyclass(module = "intellistream_datahub_sdk", name = "TenantServiceAsync")]
pub struct PyTenantServiceAsync {
    pub api_service: Arc<ApiService>,
}

#[pymethods]
impl PyTenantServiceAsync {
    /// Which optional features are enabled for your tenant.
    fn features<'py>(&self, py: Python<'py>) -> PyResult<Bound<'py, PyAny>> {
        let service = self.api_service.clone();
        future_into_py(py, async move {
            service
                .tenant
                .features()
                .await
                .map(PyTenantFeatures::from)
                .map_err(crate::datahub_err)
        })
    }

    /// What you may read and write, per settings scope (`"llm"`, …).
    fn settings_permissions<'py>(&self, py: Python<'py>) -> PyResult<Bound<'py, PyAny>> {
        let service = self.api_service.clone();
        future_into_py(py, async move {
            service
                .tenant
                .settings_permissions()
                .await
                .map(permissions_to_py)
                .map_err(crate::datahub_err)
        })
    }

    /// Your organization's model configuration. Needs the `llm` read grant.
    fn llm_settings<'py>(&self, py: Python<'py>) -> PyResult<Bound<'py, PyAny>> {
        let service = self.api_service.clone();
        future_into_py(py, async move {
            service
                .tenant
                .llm_settings()
                .await
                .map(PyTenantLlmSettings::from)
                .map_err(crate::datahub_err)
        })
    }

    /// **Replace** the model configuration: an argument left out is cleared, except `api_key`,
    /// where `None` or empty keeps the stored credential. Needs the `llm` write grant.
    #[pyo3(signature = (provider=None, model=None, api_key=None, base_url=None,
                        reasoning_effort=None, effort=None, turn_timeout=None,
                        max_output_tokens=None, max_iterations=None, instructions=None))]
    #[allow(clippy::too_many_arguments)]
    fn update_llm_settings<'py>(
        &self,
        py: Python<'py>,
        provider: Option<String>,
        model: Option<String>,
        api_key: Option<String>,
        base_url: Option<String>,
        reasoning_effort: Option<String>,
        effort: Option<String>,
        turn_timeout: Option<String>,
        max_output_tokens: Option<u32>,
        max_iterations: Option<u32>,
        instructions: Option<String>,
    ) -> PyResult<Bound<'py, PyAny>> {
        let form = llm_form(
            provider, model, api_key, base_url, reasoning_effort, effort, turn_timeout,
            max_output_tokens, max_iterations, instructions,
        );
        let service = self.api_service.clone();
        future_into_py(py, async move {
            service
                .tenant
                .update_llm_settings(&form)
                .await
                .map(PyTenantLlmSettings::from)
                .map_err(crate::datahub_err)
        })
    }
}

pub fn register(m: &Bound<'_, PyModule>) -> PyResult<()> {
    m.add_class::<PyTenantFeatures>()?;
    m.add_class::<PySettingsPermission>()?;
    m.add_class::<PyTenantLlmSettings>()?;
    m.add_class::<PyTenantServiceSync>()?;
    m.add_class::<PyTenantServiceAsync>()?;
    Ok(())
}
