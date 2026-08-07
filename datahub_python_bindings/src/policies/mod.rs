pub(crate) mod async_service;
pub(crate) mod sync_service;

use dataplatform_rust_sdk::generic::IdAndExtId;
use dataplatform_rust_sdk::policies::{
    NamingCheck, NamingCheckForm, Policy, PolicyDecision, PolicyFinding, PolicyScope, PolicyType,
};
use pyo3::exceptions::PyValueError;
use pyo3::prelude::*;
use pyo3::{pyclass, pymethods, FromPyObject, PyResult};
use std::collections::HashMap;

/// A policy node (the entity behind `client.policies`).
///
/// Build one from a template with `Policy(type=...)` or `Policy(name=..., template_id=...)`.
/// Every type except `NAMING_CONVENTION` must name a `data_set_id` — see `PolicyType.scope`.
#[pyclass(module = "datahub_sdk", name = "Policy", from_py_object)]
#[derive(Clone)]
pub struct PyPolicy {
    pub inner: Policy,
}

impl From<Policy> for PyPolicy {
    fn from(inner: Policy) -> Self {
        Self { inner }
    }
}
impl From<PyPolicy> for Policy {
    fn from(v: PyPolicy) -> Self {
        v.inner
    }
}

#[pymethods]
impl PyPolicy {
    #[new]
    #[pyo3(signature = (
        name = None,
        r#type = None,
        external_id = None,
        id = None,
        description = None,
        metadata = None,
        data_set_id = None,
        template_id = None,
        value = None,
        deactivated = false,
    ))]
    #[allow(clippy::too_many_arguments)]
    pub fn __init__(
        name: Option<String>,
        r#type: Option<PyPolicyType>,
        external_id: Option<String>,
        id: Option<u64>,
        description: Option<String>,
        metadata: Option<HashMap<String, String>>,
        data_set_id: Option<u64>,
        template_id: Option<u64>,
        value: Option<Bound<'_, PyAny>>,
        deactivated: bool,
    ) -> PyResult<Self> {
        let policy_type = r#type.map(PolicyType::from);
        // A policy needs something to identify what it is: a type, a template, or (when updating an
        // existing one) an id. Catching it here beats a 500 from the server's empty-name path.
        let name = match (name, policy_type) {
            (Some(n), _) => n,
            (None, Some(t)) => t.as_str().to_string(),
            (None, None) => {
                if id.is_none() && template_id.is_none() {
                    return Err(PyValueError::new_err(
                        "Policy needs a name, a type, a template_id, or an id",
                    ));
                }
                String::new()
            }
        };
        let value = match value {
            Some(v) => Some(pythonize_to_json(&v)?),
            None => None,
        };
        Ok(Self {
            inner: Policy {
                id,
                external_id,
                name,
                description,
                source: None,
                metadata: metadata.unwrap_or_default(),
                labels: Vec::new(),
                policy_type,
                value,
                deactivated,
                node_type: None,
                template_id,
                data_set_id,
                created_time: None,
                last_updated_time: None,
            },
        })
    }

    #[getter]
    fn id(&self) -> Option<u64> {
        self.inner.id
    }
    #[getter]
    fn external_id(&self) -> Option<&str> {
        self.inner.external_id.as_deref()
    }
    #[setter]
    fn set_external_id(&mut self, value: Option<String>) {
        self.inner.external_id = value;
    }
    #[getter]
    fn name(&self) -> &str {
        self.inner.name.as_str()
    }
    #[setter]
    fn set_name(&mut self, value: String) {
        self.inner.name = value;
    }
    #[getter]
    fn description(&self) -> Option<&str> {
        self.inner.description.as_deref()
    }
    #[setter]
    fn set_description(&mut self, value: Option<String>) {
        self.inner.description = value;
    }
    #[getter]
    fn metadata(&self) -> HashMap<String, String> {
        self.inner.metadata.clone()
    }
    #[setter]
    fn set_metadata(&mut self, value: HashMap<String, String>) {
        self.inner.metadata = value;
    }
    #[getter]
    fn labels(&self) -> Vec<String> {
        self.inner.labels.clone()
    }
    #[getter]
    fn r#type(&self) -> Option<PyPolicyType> {
        self.inner.policy_type.map(PyPolicyType::from)
    }
    #[setter]
    fn set_type(&mut self, value: Option<PyPolicyType>) {
        self.inner.policy_type = value.map(PolicyType::from);
    }
    /// The policy's configured value, as a plain Python object. Shape depends on the type.
    #[getter]
    fn value<'py>(&self, py: Python<'py>) -> PyResult<Option<Bound<'py, PyAny>>> {
        self.inner
            .value
            .as_ref()
            .map(|v| json_to_python(py, v))
            .transpose()
    }
    #[getter]
    fn deactivated(&self) -> bool {
        self.inner.deactivated
    }
    #[setter]
    fn set_deactivated(&mut self, value: bool) {
        self.inner.deactivated = value;
    }
    #[getter]
    fn template_id(&self) -> Option<u64> {
        self.inner.template_id
    }
    #[setter]
    fn set_template_id(&mut self, value: Option<u64>) {
        self.inner.template_id = value;
    }
    /// The dataset this policy governs; `None` means tenant-wide.
    #[getter]
    fn data_set_id(&self) -> Option<u64> {
        self.inner.data_set_id
    }
    #[setter]
    fn set_data_set_id(&mut self, value: Option<u64>) {
        self.inner.data_set_id = value;
    }

    fn __repr__(&self) -> String {
        format!(
            "Policy(id={:?}, external_id={:?}, name={:?}, type={:?}, data_set_id={:?})",
            self.inner.id,
            self.inner.external_id,
            self.inner.name,
            self.inner.policy_type.map(|t| t.as_str()),
            self.inner.data_set_id
        )
    }
}

/// The policy templates the server offers. Mirrors the server's `PolicyType` enum.
#[pyclass(module = "datahub_sdk", name = "PolicyType", eq, eq_int, frozen, hash)]
#[derive(Clone, Copy, PartialEq, Eq, Hash, Debug)]
pub enum PyPolicyType {
    SECURITY_POLICY,
    ENCRYPTION_POLICY,
    MASKING_POLICY,
    IS_WRITE_PROTECTED,
    IS_READ_PROTECTED,
    HAS_REQUIREMENT,
    NAMING_CONVENTION,
}

impl From<PolicyType> for PyPolicyType {
    fn from(t: PolicyType) -> Self {
        match t {
            PolicyType::SecurityPolicy => PyPolicyType::SECURITY_POLICY,
            PolicyType::EncryptionPolicy => PyPolicyType::ENCRYPTION_POLICY,
            PolicyType::MaskingPolicy => PyPolicyType::MASKING_POLICY,
            PolicyType::IsWriteProtected => PyPolicyType::IS_WRITE_PROTECTED,
            PolicyType::IsReadProtected => PyPolicyType::IS_READ_PROTECTED,
            PolicyType::HasRequirement => PyPolicyType::HAS_REQUIREMENT,
            PolicyType::NamingConvention => PyPolicyType::NAMING_CONVENTION,
        }
    }
}

impl From<PyPolicyType> for PolicyType {
    fn from(t: PyPolicyType) -> Self {
        match t {
            PyPolicyType::SECURITY_POLICY => PolicyType::SecurityPolicy,
            PyPolicyType::ENCRYPTION_POLICY => PolicyType::EncryptionPolicy,
            PyPolicyType::MASKING_POLICY => PolicyType::MaskingPolicy,
            PyPolicyType::IS_WRITE_PROTECTED => PolicyType::IsWriteProtected,
            PyPolicyType::IS_READ_PROTECTED => PolicyType::IsReadProtected,
            PyPolicyType::HAS_REQUIREMENT => PolicyType::HasRequirement,
            PyPolicyType::NAMING_CONVENTION => PolicyType::NamingConvention,
        }
    }
}

#[pymethods]
impl PyPolicyType {
    /// Where a policy of this type may be attached.
    #[getter]
    fn scope(&self) -> PyPolicyScope {
        PolicyType::from(*self).scope().into()
    }
    /// Whether this type may name a `data_set_id`.
    fn can_attach_to_data_set(&self) -> bool {
        PolicyType::from(*self).can_attach_to_data_set()
    }
    /// Whether this type may be left tenant-wide (no `data_set_id`).
    fn can_apply_tenant_wide(&self) -> bool {
        PolicyType::from(*self).can_apply_tenant_wide()
    }
    /// The wire name, as the server spells it.
    fn __str__(&self) -> &'static str {
        PolicyType::from(*self).as_str()
    }
}

/// Where a `PolicyType` may be attached.
#[pyclass(module = "datahub_sdk", name = "PolicyScope", eq, eq_int, frozen, hash)]
#[derive(Clone, Copy, PartialEq, Eq, Hash, Debug)]
pub enum PyPolicyScope {
    TENANT_ONLY,
    DATASET_ONLY,
    TENANT_WITH_DATASET_OVERRIDE,
}

impl From<PolicyScope> for PyPolicyScope {
    fn from(s: PolicyScope) -> Self {
        match s {
            PolicyScope::TenantOnly => PyPolicyScope::TENANT_ONLY,
            PolicyScope::DatasetOnly => PyPolicyScope::DATASET_ONLY,
            PolicyScope::TenantWithDatasetOverride => PyPolicyScope::TENANT_WITH_DATASET_OVERRIDE,
        }
    }
}

/// A policy's verdict on one submitted id.
#[pyclass(module = "datahub_sdk", name = "PolicyFinding")]
#[derive(Clone)]
pub struct PyPolicyFinding {
    pub inner: PolicyFinding,
}

#[pymethods]
impl PyPolicyFinding {
    /// Position in the submitted batch, so a rejection can name which id was wrong.
    #[getter]
    fn index(&self) -> usize {
        self.inner.index
    }
    /// The offending value, verbatim as submitted.
    #[getter]
    fn external_id(&self) -> Option<&str> {
        self.inner.external_id.as_deref()
    }
    #[getter]
    fn decision(&self) -> PyPolicyDecision {
        self.inner.decision.into()
    }
    /// External id of the policy that fired — the rule, not just the symptom.
    #[getter]
    fn policy(&self) -> Option<&str> {
        self.inner.policy_external_id.as_deref()
    }
    #[getter]
    fn message(&self) -> Option<&str> {
        self.inner.message.as_deref()
    }
    /// A conforming alternative where one could be derived, for the caller to accept. The platform
    /// deliberately does not rewrite external ids on its own.
    #[getter]
    fn suggestion(&self) -> Option<&str> {
        self.inner.suggestion.as_deref()
    }
    /// Whether this finding would block a write.
    #[getter]
    fn is_rejection(&self) -> bool {
        self.inner.decision == PolicyDecision::NotOk
    }

    fn __repr__(&self) -> String {
        format!(
            "PolicyFinding(index={}, external_id={:?}, decision={:?}, policy={:?})",
            self.inner.index,
            self.inner.external_id,
            self.inner.decision,
            self.inner.policy_external_id
        )
    }
}

#[pyclass(
    module = "datahub_sdk",
    name = "PolicyDecision",
    eq,
    eq_int,
    frozen,
    hash
)]
#[derive(Clone, Copy, PartialEq, Eq, Hash, Debug)]
pub enum PyPolicyDecision {
    OK,
    WARNING,
    NOT_OK,
}

impl From<PolicyDecision> for PyPolicyDecision {
    fn from(d: PolicyDecision) -> Self {
        match d {
            PolicyDecision::Ok => PyPolicyDecision::OK,
            PolicyDecision::Warning => PyPolicyDecision::WARNING,
            PolicyDecision::NotOk => PyPolicyDecision::NOT_OK,
        }
    }
}

/// The answer from `policies.check_naming`. Only non-conforming ids appear in `findings`.
#[pyclass(module = "datahub_sdk", name = "NamingCheck")]
#[derive(Clone)]
pub struct PyNamingCheck {
    pub inner: NamingCheck,
}

impl From<NamingCheck> for PyNamingCheck {
    fn from(inner: NamingCheck) -> Self {
        Self { inner }
    }
}

#[pymethods]
impl PyNamingCheck {
    #[getter]
    fn findings(&self) -> Vec<PyPolicyFinding> {
        self.inner
            .findings
            .iter()
            .map(|f| PyPolicyFinding { inner: f.clone() })
            .collect()
    }
    /// True when nothing was flagged.
    #[getter]
    fn is_clean(&self) -> bool {
        self.inner.is_clean()
    }
    /// Only the findings that would block a write, as opposed to merely warn.
    #[getter]
    fn rejections(&self) -> Vec<PyPolicyFinding> {
        self.inner
            .rejections()
            .map(|f| PyPolicyFinding { inner: f.clone() })
            .collect()
    }

    fn __len__(&self) -> usize {
        self.inner.findings.len()
    }
    fn __bool__(&self) -> bool {
        !self.inner.is_clean()
    }
    fn __repr__(&self) -> String {
        format!("NamingCheck({} findings)", self.inner.findings.len())
    }
}

/// Candidate ids to run the naming policy over.
///
/// `external_ids` is required and must not be empty. `names` is optional and, when given, must
/// line up one-for-one with `external_ids` — it only helps the server derive a better
/// `suggestion`, and is not a second list of things to check.
#[pyclass(module = "datahub_sdk", name = "NamingCheckForm", from_py_object)]
#[derive(Clone)]
pub struct PyNamingCheckForm {
    pub inner: NamingCheckForm,
}

impl From<PyNamingCheckForm> for NamingCheckForm {
    fn from(v: PyNamingCheckForm) -> Self {
        v.inner
    }
}

#[pymethods]
impl PyNamingCheckForm {
    #[new]
    #[pyo3(signature = (external_ids, names = None, data_set_id = None))]
    pub fn __init__(
        external_ids: Vec<String>,
        names: Option<Vec<String>>,
        data_set_id: Option<u64>,
    ) -> PyResult<Self> {
        if external_ids.is_empty() {
            return Err(PyValueError::new_err(
                "external_ids must not be empty — the server rejects an empty check with 400",
            ));
        }
        let names = names.unwrap_or_default();
        if !names.is_empty() && names.len() != external_ids.len() {
            return Err(PyValueError::new_err(format!(
                "names must be omitted or line up with external_ids ({} names for {} ids)",
                names.len(),
                external_ids.len()
            )));
        }
        Ok(Self {
            inner: NamingCheckForm {
                external_ids,
                names,
                data_set_id,
            },
        })
    }

    #[getter]
    fn external_ids(&self) -> Vec<String> {
        self.inner.external_ids.clone()
    }
    #[getter]
    fn names(&self) -> Vec<String> {
        self.inner.names.clone()
    }
    #[getter]
    fn data_set_id(&self) -> Option<u64> {
        self.inner.data_set_id
    }
}

/// Things accepted as a policy identifier when deleting: a `Policy`, its numeric id, or its
/// external id. Mirrors `LabelIdentifiable`.
#[derive(Clone, FromPyObject)]
pub enum PolicyIdentifiable {
    Policy(PyPolicy),
    ExternalId(String),
    Id(u64),
}

impl From<PolicyIdentifiable> for IdAndExtId {
    fn from(value: PolicyIdentifiable) -> Self {
        match value {
            PolicyIdentifiable::Policy(p) => Self {
                id: p.inner.id,
                external_id: p.inner.external_id.clone(),
            },
            PolicyIdentifiable::ExternalId(ext) => Self {
                id: None,
                external_id: Some(ext),
            },
            PolicyIdentifiable::Id(id) => Self {
                id: Some(id),
                external_id: None,
            },
        }
    }
}

/// `policies.update` sends a whole policy, and the server reads only the first item in the
/// wrapper — so a multi-item call would silently drop everything after the first.
pub(crate) fn require_single(policies: &[PyPolicy]) -> PyResult<()> {
    if policies.len() > 1 {
        return Err(PyValueError::new_err(
            "policies.update accepts one policy per call — the server only reads the first item",
        ));
    }
    if policies.is_empty() {
        return Err(PyValueError::new_err("policies.update needs a policy"));
    }
    Ok(())
}

/// Convert a Python object to `serde_json::Value` for `Policy.value`, whose shape depends on the
/// policy type and so cannot be typed here.
fn pythonize_to_json(obj: &Bound<'_, PyAny>) -> PyResult<serde_json::Value> {
    let json = PyModule::import(obj.py(), "json")?;
    let dumped: String = json.call_method1("dumps", (obj,))?.extract()?;
    serde_json::from_str(&dumped).map_err(|e| PyValueError::new_err(e.to_string()))
}

fn json_to_python<'py>(py: Python<'py>, value: &serde_json::Value) -> PyResult<Bound<'py, PyAny>> {
    let json = PyModule::import(py, "json")?;
    let text = value.to_string();
    json.call_method1("loads", (text,))
}

pub fn register(m: &Bound<'_, PyModule>) -> PyResult<()> {
    m.add_class::<PyPolicy>()?;
    m.add_class::<PyPolicyType>()?;
    m.add_class::<PyPolicyScope>()?;
    m.add_class::<PyPolicyDecision>()?;
    m.add_class::<PyPolicyFinding>()?;
    m.add_class::<PyNamingCheck>()?;
    m.add_class::<PyNamingCheckForm>()?;
    m.add_class::<sync_service::PyPoliciesServiceSync>()?;
    m.add_class::<async_service::PyPoliciesServiceAsync>()?;
    Ok(())
}
