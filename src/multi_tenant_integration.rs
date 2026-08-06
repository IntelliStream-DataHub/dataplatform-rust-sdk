//! Multi-organization and dataset-ACL integration tests.
//!
//! Everything here is `#[ignore]`d and needs a live backend plus a Keycloak realm carrying six
//! purpose-built principals. Run with:
//!
//! ```text
//! cargo test multi_tenant -- --ignored --nocapture
//! cargo test acl          -- --ignored --nocapture
//! ```
//!
//! A test whose fixture is missing from `.env` prints `SKIP …` and passes, so a checkout without
//! the realm setup is unaffected.
//!
//! # What the backend actually does
//!
//! The SDK carries tenant identity **only** in the access token's `organization` claim — there is
//! no tenant header, query param or per-call override, and one [`DataHubConfig`] owns one token
//! cache, so one client is one tenant. The only lever is the OAuth2 `scope`.
//!
//! `SecurityConfig.OrganizationValidator` on the API side runs inside the `JwtDecoder`, so every
//! rejection is a **401** raised before any controller:
//!
//! | `organization` claim | Result |
//! |---|---|
//! | absent (no `organization:<selector>` in scope) | 401 |
//! | a JSON array (two mappers writing the claim, or `addOrganizationId` off) | 401 |
//! | more than one entry (a multi-org principal asking for `organization:*`) | 401 |
//! | exactly one entry with an `id` | accepted; that organization is the tenant |
//!
//! So a principal belonging to several organizations is **refused outright** rather than assigned
//! one of them, and must pin `organization:<alias>` — which is also how it switches tenant.
//!
//! **The reason never reaches the client.** The API installs a custom authentication entry point
//! that emits a bare `WWW-Authenticate: Bearer realm="Restricted Content"` with no
//! `error_description`, and a body that is Spring's generic error JSON; the descriptive message
//! only goes to the server log. Every 401 assertion below is therefore on the **status alone**, and
//! the cases are told apart by how the fixture is built. Don't waste time trying to assert on the
//! message — there isn't one. (403s are different: those carry an RFC 9457 `problem+json` body,
//! which these tests do assert on.)
//!
//! Tenant isolation is physical — one Postgres database per organization, no tenant column
//! anywhere. Two organizations can therefore each own the same `externalId`, and numeric ids are
//! per-tenant sequences, so id 42 names *different real entities* in two tenants. These tests only
//! ever cross tenants by external id; a numeric-id probe would be meaningless.
//!
//! Dataset ACLs are Keycloak **organization groups** named after the dataset's `externalId`, read
//! out of the UserInfo endpoint (not the token) and cached for ~10s in-process. Read and write are
//! independent — a write grant does not confer read. Denials are 403 with `problem+json` carrying
//! `dataSetId` and `permission`, *except* for lists and searches, which are narrowed in SQL and
//! come back **200 with the rows silently missing**.
//!
//! # The fixtures
//!
//! Six client-credentials principals. `MT_ORG_A_*` must be a full-access principal **in the same
//! organization as D/E/F** — it is the admin that seeds data for the ACL tests. `MT_ORG_B_*` is a
//! full-access principal in a *different* organization, used only to prove isolation.
//!
//! | Env prefix | Organizations | Dataset grant | Realm roles |
//! |---|---|---|---|
//! | `MT_ORG_A` | the ACL org | — | `DATAHUB_ACCESS`, `DATAHUB_DATASET_ALL` |
//! | `MT_ORG_B` | a second org | — | `DATAHUB_ACCESS`, `DATAHUB_DATASET_ALL` |
//! | `MT_MULTI` | **both** of the above | — | `DATAHUB_ACCESS`, `DATAHUB_DATASET_ALL` |
//! | `MT_READONLY` | the ACL org | `/datasets/<ds>/read` | `DATAHUB_ACCESS` only |
//! | `MT_WRITEONLY` | the ACL org | `/datasets/<ds>/write` | `DATAHUB_ACCESS` only |
//! | `MT_NOGRANT` | the ACL org | none | `DATAHUB_ACCESS` only |
//!
//! The read/write/no-grant principals must **not** hold `DATAHUB_DATASET_ALL`, `DATAHUB_ADMIN`,
//! `DATAHUB_DATASET_READ_ALL` or `DATAHUB_DATASET_WRITE_ALL` — any of those makes the group grant
//! moot and collapses every ACL test into "allowed".
//!
//! ```text
//! MT_ORG_A_ALIAS / MT_ORG_B_ALIAS      the two organization aliases MT_MULTI belongs to
//! MT_ORG_A_CLIENT_ID     / _SECRET     A — full access, ACL org
//! MT_ORG_B_CLIENT_ID     / _SECRET     B — full access, other org
//! MT_MULTI_CLIENT_ID     / _SECRET     C — member of both
//! MT_READONLY_CLIENT_ID  / _SECRET     D
//! MT_WRITEONLY_CLIENT_ID / _SECRET     E
//! MT_NOGRANT_CLIENT_ID   / _SECRET     F
//! MT_DATASET_EXT_ID                    dataset D and E are granted on; must exist in the ACL org
//! ```
//!
//! `BASE_URL` and `TOKEN_URI` come from the same `.env` as the rest of the suite.
//!
//! # Setting the realm up
//!
//! Two realm-wide prerequisites on the built-in `organization` client scope, without which nothing
//! below works. Both are documented in the platform repo's `datahub-api/KEYCLOAK_ORG_GROUPS.md`;
//! `deploy/keycloak/bootstrap-org-groups.sh` applies them.
//!
//! 1. `addOrganizationId=true` on the `oidc-organization-membership-mapper` — off by default, and
//!    without it the claim is a flat array of aliases that the API rejects.
//! 2. An `oidc-organization-group-membership-mapper` with `userinfo.token.claim=true` — without it
//!    every caller silently has *zero* dataset grants and D, E and F become indistinguishable.
//!
//! Each principal must also carry exactly **one** mapper writing the `organization` claim. A client
//! that has both the built-in `organization` scope and a bespoke one (the platform's `tenant-org`,
//! say) gets the two values merged into a JSON array, and every call 401s.
//!
//! Dataset grants are a three-level organization-group tree, one leaf per permission:
//!
//! ```text
//! datasets                  top-level organization group
//! └── <dataset externalId>  verbatim, matched case-insensitively
//!     ├── read
//!     └── write
//! ```
//!
//! Membership goes on the **leaf** — UserInfo lists only the groups a user is directly in, so
//! joining `datasets` or the dataset node grants nothing. A malformed path is silently ignored
//! rather than rejected, so a typo reads as "no grant". Organization groups have their own admin
//! endpoints (`/organizations/{orgId}/groups`, nesting through `/children`); the ordinary
//! realm-groups API refuses them outright. For a service account the user to add is
//! `service-account-<clientId>`.
//!
//! Verify a principal before trusting a failure here — decode its token and read UserInfo:
//!
//! ```text
//! TOK=$(curl -s $TOKEN_URI -d grant_type=client_credentials \
//!        -d client_id=$ID -d client_secret=$SECRET -d 'scope=openid organization:*' \
//!      | python3 -c 'import sys,json;print(json.load(sys.stdin)["access_token"])')
//! curl -s ${TOKEN_URI%/token}/userinfo -H "Authorization: Bearer $TOK"
//! ```
//!
//! `organization` must be an **object** keyed by alias (one entry, or two for `MT_MULTI`), and
//! UserInfo must additionally show `organization.<alias>.groups` for D and E.

use crate::datahub::DataHubConfig;
use crate::datasets::Dataset;
use crate::generic::{DataWrapper, IdAndExtId, SearchAndFilterForm, SearchForm};
use crate::graph_data_wrapper::GraphDataWrapper;
use crate::http::ResponseError;
use crate::resources::{RelatedResourcesForm, Resource};
use crate::tests::cleanup::{cleanup_datasets_as, cleanup_resources_as, cleanup_timeseries_as};
use crate::{ApiService, TimeSeries};
use chrono::Utc;
use std::sync::{Arc, Once};
use uuid::Uuid;

/// Ask the token endpoint for every organization the caller belongs to. Resolves cleanly for a
/// single-organization principal; ambiguous, and therefore rejected, for a multi-organization one.
const SCOPE_ALL_ORGS: &str = "openid organization:*";

/// Pin one organization. The only scope a multi-organization principal can use.
fn scope_for(alias: &str) -> String {
    format!("openid organization:{alias}")
}

static LOAD_ENV: Once = Once::new();

/// Read a `.env`-or-environment value, treating blank as unset.
///
/// Reads only — never `set_var`. `#[tokio::test]`s share one process, and mutating the environment
/// from one while another snapshots it through [`DataHubConfig::from_env`] is a race.
fn env_var(key: &str) -> Option<String> {
    LOAD_ENV.call_once(|| {
        dotenv::dotenv().ok();
    });
    std::env::var(key)
        .ok()
        .map(|v| v.trim().to_string())
        .filter(|v| !v.is_empty())
}

/// One configured principal: a service to call with, plus the config it was built from.
///
/// The config is kept because teardown needs to rebuild an identical client on its own runtime
/// (see [`crate::tests::cleanup`]) — the data belongs to this principal's tenant and no other
/// identity can delete it.
struct Principal {
    service: Arc<ApiService>,
    config: DataHubConfig,
    label: String,
}

/// Build the principal at `prefix`, or print a skip note and return `None`.
///
/// `scope` is `None` to send no `scope` parameter at all — the "I forgot to set `SCOPE`" case,
/// which yields a token with no `organization` claim.
fn principal(test: &str, prefix: &str, scope: Option<&str>) -> Option<Principal> {
    let mut missing = Vec::new();
    let mut required = |key: String| -> Option<String> {
        let value = env_var(&key);
        if value.is_none() {
            missing.push(key);
        }
        value
    };
    let base_url = required("BASE_URL".to_string());
    let token_uri = required("TOKEN_URI".to_string());
    let client_id = required(format!("{prefix}_CLIENT_ID"));
    let client_secret = required(format!("{prefix}_CLIENT_SECRET"));

    let (Some(base_url), Some(token_uri), Some(client_id), Some(client_secret)) =
        (base_url, token_uri, client_id, client_secret)
    else {
        eprintln!("SKIP {test}: not configured — missing {}", missing.join(", "));
        return None;
    };

    let mut config = DataHubConfig::from_vars(
        base_url,
        None,
        Some(token_uri),
        Some(client_id.clone()),
        Some(client_secret),
        None,
    );
    if let Some(scope) = scope {
        config.set_scope(scope);
    }
    Some(Principal {
        service: ApiService::new(config.clone()),
        config,
        label: format!("{prefix} ({client_id})"),
    })
}

impl Principal {
    /// The same principal with durable ingest buffering pointed at `dir`.
    fn buffered(&self, dir: std::path::PathBuf) -> Principal {
        let mut config = self.config.clone();
        config.set_buffer_dir(dir).set_buffer_retention_secs(3600);
        Principal {
            service: ApiService::new(config.clone()),
            config,
            label: self.label.clone(),
        }
    }
}

/// A distinct external id per run. Entities are per-tenant, but the ACL tests seed and read across
/// principals in one tenant, so collisions between concurrent runs are real.
fn unique_id(kind: &str) -> String {
    format!("rust_sdk_mt_{}_{}", kind, &Uuid::new_v4().to_string()[..12])
}

fn resource(external_id: &str, data_set_id: Option<u64>) -> Resource {
    let mut r = Resource::new();
    r.external_id = external_id.to_string();
    r.name = external_id.to_string();
    r.source = Some("rust_sdk_multi_tenant_test".to_string());
    r.labels = Some(vec!["ASSET".to_string()]);
    r.data_set_id = data_set_id;
    r
}

fn by_external_id(external_id: &str) -> Vec<IdAndExtId> {
    vec![IdAndExtId::from_external_id(external_id)]
}

/// Assert a call was refused with `expected`, reporting what came back instead.
fn assert_status<T>(result: Result<T, ResponseError>, expected: u16, context: &str) -> ResponseError
where
    T: std::fmt::Debug,
{
    match result {
        Ok(ok) => panic!("{context}: expected HTTP {expected}, but the call succeeded: {ok:?}"),
        Err(e) => {
            assert_eq!(
                e.get_status().as_u16(),
                expected,
                "{context}: expected HTTP {expected}, got {} — body: {}",
                e.get_status(),
                e.get_message()
            );
            e
        }
    }
}

/// Whether a `byids`-style read found nothing.
///
/// Two shapes count as "not there", and which one you get depends on the endpoint: a bulk read
/// narrowed by the tenant's database (or by the dataset ACL) answers 200 with an empty node list,
/// while a single-item read of something in another tenant is a 404. Both mean the caller cannot
/// see it, which is what these tests are about.
fn is_absent(result: &Result<GraphDataWrapper<Resource>, ResponseError>) -> bool {
    match result {
        Ok(wrapper) => wrapper.nodes().map_or(true, |n| n.is_empty()),
        Err(e) => e.get_status().as_u16() == 404,
    }
}

/// Resolve the numeric id of the dataset the ACL fixtures grant on.
async fn acl_dataset_id(admin: &Principal, external_id: &str) -> u64 {
    let found = admin
        .datasets_by_external_id(external_id)
        .await
        .unwrap_or_else(|e| {
            // A 401 here is the realm, not the ACL: the seeding principal's own token was
            // refused, so nothing below this point would mean anything. The body is empty on a
            // 401, hence leading with the status.
            panic!(
                "MT_DATASET_EXT_ID='{external_id}' could not be read as {}: HTTP {} {}",
                admin.label,
                e.get_status(),
                e.get_message()
            )
        });
    found.unwrap_or_else(|| {
        panic!(
            "MT_DATASET_EXT_ID='{external_id}' does not exist in {}'s tenant — the ACL fixtures \
             grant on a dataset that isn't there",
            admin.label
        )
    })
}

impl Principal {
    async fn datasets_by_external_id(
        &self,
        external_id: &str,
    ) -> Result<Option<u64>, ResponseError> {
        let found = self
            .service
            .datasets
            .by_ids(&by_external_id(external_id))
            .await?;
        Ok(found.get_items().first().and_then(|d| d.id))
    }
}

/// Fail loudly if even a single-organization token is refused.
///
/// The 401 assertions below are otherwise vacuous: a realm that rejects *everything* — two mappers
/// merging the `organization` claim into an array, `addOrganizationId` left off, the tenant not
/// provisioned on the API — produces exactly the 401 those tests are looking for, and they would
/// pass while proving nothing. So they check first that a well-formed token does get through.
///
/// Returns `false` (having printed the skip note) when the control principal isn't configured.
async fn require_healthy_realm(test: &str) -> bool {
    let Some(control) = principal(test, "MT_ORG_A", Some(SCOPE_ALL_ORGS)) else {
        return false;
    };
    if let Err(e) = control.service.units.list().await {
        panic!(
            "{test}: the control principal {} is refused with HTTP {} under '{SCOPE_ALL_ORGS}', so \
             a 401 here would prove nothing. Fix the realm first: exactly one mapper may write the \
             `organization` claim, `addOrganizationId` must be on, and the organization must be \
             provisioned as a tenant on the API.",
            control.label,
            e.get_status()
        );
    }
    true
}

// ---------------------------------------------------------------------------------------------
// Multi-organization token resolution
// ---------------------------------------------------------------------------------------------

/// A principal in two organizations asking for all of them is refused, not silently assigned one.
///
/// This is the whole reason the suite exists: `organization:*` is the natural default for a client
/// that serves many tenants, and it works right up until someone joins a second organization.
#[tokio::test]
#[ignore]
async fn multi_tenant_multi_org_principal_with_wildcard_scope_is_rejected(
) -> Result<(), ResponseError> {
    const TEST: &str = "multi_org_principal_with_wildcard_scope_is_rejected";
    if !require_healthy_realm(TEST).await {
        return Ok(());
    }
    let Some(multi) = principal(TEST, "MT_MULTI", Some(SCOPE_ALL_ORGS)) else {
        return Ok(());
    };

    // Status only — the API deliberately drops the "Ambiguous organization context" description
    // before it reaches us. See the module docs.
    assert_status(
        multi.service.units.list().await,
        401,
        "a token naming two organizations",
    );
    Ok(())
}

/// The same principal with no organization selector at all: the claim is absent, so also a 401.
///
/// This is the shape of the "I forgot to set `SCOPE`" mistake, and it is worth pinning separately
/// because it is indistinguishable from bad credentials at the call site.
#[tokio::test]
#[ignore]
async fn multi_tenant_principal_without_an_organization_selector_is_rejected(
) -> Result<(), ResponseError> {
    const TEST: &str = "principal_without_an_organization_selector_is_rejected";
    if !require_healthy_realm(TEST).await {
        return Ok(());
    }
    let Some(multi) = principal(TEST, "MT_MULTI", None) else {
        return Ok(());
    };

    assert_status(
        multi.service.units.list().await,
        401,
        "a token with no organization claim",
    );
    Ok(())
}

/// The control: `organization:*` is fine for a principal in exactly one organization.
///
/// Without this, the rejection above could just as well mean the scope form is wrong.
#[tokio::test]
#[ignore]
async fn multi_tenant_single_org_principal_with_wildcard_scope_succeeds() -> Result<(), ResponseError>
{
    let Some(single) = principal(
        "single_org_principal_with_wildcard_scope_succeeds",
        "MT_ORG_A",
        Some(SCOPE_ALL_ORGS),
    ) else {
        return Ok(());
    };

    let units = single.service.units.list().await?;
    assert!(
        !units.get_items().is_empty(),
        "{} should be able to list units under organization:*",
        single.label
    );
    Ok(())
}

/// Pinning an alias both unblocks the multi-org principal *and* selects which tenant it reaches.
///
/// Passing validation is only half the claim — the interesting part is that the two pins land in
/// different databases, which is what makes `organization:<alias>` a tenant switch rather than
/// merely a way to quiet the validator.
#[tokio::test]
#[ignore]
async fn multi_tenant_pinning_an_alias_selects_that_tenant() -> Result<(), ResponseError> {
    const TEST: &str = "pinning_an_alias_selects_that_tenant";
    let (Some(alias_a), Some(alias_b)) = (env_var("MT_ORG_A_ALIAS"), env_var("MT_ORG_B_ALIAS"))
    else {
        eprintln!("SKIP {TEST}: MT_ORG_A_ALIAS / MT_ORG_B_ALIAS not configured");
        return Ok(());
    };
    let (Some(pinned_a), Some(pinned_b)) = (
        principal(TEST, "MT_MULTI", Some(&scope_for(&alias_a))),
        principal(TEST, "MT_MULTI", Some(&scope_for(&alias_b))),
    ) else {
        return Ok(());
    };

    let external_id = unique_id("pin");
    let mut guard = cleanup_resources_as(pinned_a.config.clone(), vec![external_id.clone()]);

    let created = pinned_a
        .service
        .resources
        .create(vec![resource(&external_id, None)], vec![])
        .await?;
    assert_eq!(
        created.nodes().map_or(0, |n| n.len()),
        1,
        "pinned to '{alias_a}', the multi-org principal should be able to write"
    );

    let from_a = pinned_a
        .service
        .resources
        .by_ids(&by_external_id(&external_id))
        .await;
    assert!(
        !is_absent(&from_a),
        "pinned to '{alias_a}', the principal should see what it just created"
    );

    let from_b = pinned_b
        .service
        .resources
        .by_ids(&by_external_id(&external_id))
        .await;
    assert!(
        is_absent(&from_b),
        "pinned to '{alias_b}', the same principal must not see '{alias_a}' data — got {from_b:?}"
    );

    pinned_a
        .service
        .resources
        .delete(&by_external_id(&external_id))
        .await?;
    guard.disarm();
    Ok(())
}

// ---------------------------------------------------------------------------------------------
// Tenant isolation
// ---------------------------------------------------------------------------------------------

/// Two organizations can each own the same `externalId`, and the two entities are unrelated.
///
/// Uniqueness is a per-tenant-database constraint, so this is isolation by construction rather
/// than by a filter someone could forget to apply.
#[tokio::test]
#[ignore]
async fn multi_tenant_same_external_id_in_two_orgs_are_independent() -> Result<(), ResponseError> {
    const TEST: &str = "same_external_id_in_two_orgs_are_independent";
    let (Some(org_a), Some(org_b)) = (
        principal(TEST, "MT_ORG_A", Some(SCOPE_ALL_ORGS)),
        principal(TEST, "MT_ORG_B", Some(SCOPE_ALL_ORGS)),
    ) else {
        return Ok(());
    };

    let external_id = unique_id("shared");
    let mut guard_a = cleanup_resources_as(org_a.config.clone(), vec![external_id.clone()]);
    let guard_b = cleanup_resources_as(org_b.config.clone(), vec![external_id.clone()]);

    let mut in_a = resource(&external_id, None);
    in_a.description = Some("belongs to org A".to_string());
    let mut in_b = resource(&external_id, None);
    in_b.description = Some("belongs to org B".to_string());

    org_a.service.resources.create(vec![in_a], vec![]).await?;
    // The same external id in the other tenant must not collide with the one just created.
    org_b.service.resources.create(vec![in_b], vec![]).await?;

    let read_a = org_a
        .service
        .resources
        .by_ids(&by_external_id(&external_id))
        .await?;
    let read_b = org_b
        .service
        .resources
        .by_ids(&by_external_id(&external_id))
        .await?;
    assert_eq!(
        read_a.nodes().and_then(|n| n.first().and_then(|r| r.description.clone())),
        Some("belongs to org A".to_string()),
        "org A must read back its own entity"
    );
    assert_eq!(
        read_b.nodes().and_then(|n| n.first().and_then(|r| r.description.clone())),
        Some("belongs to org B".to_string()),
        "org B must read back its own entity, not org A's"
    );

    // Deleting one leaves the other standing.
    org_a
        .service
        .resources
        .delete(&by_external_id(&external_id))
        .await?;
    guard_a.disarm();

    let still_in_b = org_b
        .service
        .resources
        .by_ids(&by_external_id(&external_id))
        .await;
    assert!(
        !is_absent(&still_in_b),
        "deleting org A's entity must not touch org B's entity of the same external id"
    );
    drop(guard_b);
    Ok(())
}

/// An entity created in one organization does not surface in the other's search.
///
/// Deliberately probed by external id and by search, never by numeric id: ids are per-tenant
/// identity sequences, so the same number names a different real entity in the other tenant and a
/// cross-tenant id lookup would prove nothing.
#[tokio::test]
#[ignore]
async fn multi_tenant_entity_created_in_one_org_is_invisible_from_the_other(
) -> Result<(), ResponseError> {
    const TEST: &str = "entity_created_in_one_org_is_invisible_from_the_other";
    let (Some(org_a), Some(org_b)) = (
        principal(TEST, "MT_ORG_A", Some(SCOPE_ALL_ORGS)),
        principal(TEST, "MT_ORG_B", Some(SCOPE_ALL_ORGS)),
    ) else {
        return Ok(());
    };

    let external_id = unique_id("isolated");
    let mut guard = cleanup_resources_as(org_a.config.clone(), vec![external_id.clone()]);
    org_a
        .service
        .resources
        .create(vec![resource(&external_id, None)], vec![])
        .await?;

    let from_b = org_b
        .service
        .resources
        .by_ids(&by_external_id(&external_id))
        .await;
    assert!(
        is_absent(&from_b),
        "org B must not read org A's entity by external id — got {from_b:?}"
    );

    let mut search = SearchAndFilterForm::new();
    let mut form = SearchForm::new();
    form.query = Some(external_id.clone());
    search.search = Some(form);
    let hits = org_b.service.resources.search(&search).await?;
    assert!(
        !hits
            .get_items()
            .iter()
            .any(|r| r.external_id == external_id),
        "org B's search must not surface org A's entity"
    );

    org_a
        .service
        .resources
        .delete(&by_external_id(&external_id))
        .await?;
    guard.disarm();
    Ok(())
}

// ---------------------------------------------------------------------------------------------
// Dataset ACL
// ---------------------------------------------------------------------------------------------

/// Set up the ACL scenario: the seeding admin, the dataset id, and a resource inside that dataset.
///
/// Returns `None` when the fixtures aren't configured, having already printed the skip note.
async fn acl_scenario(test: &str) -> Option<(Principal, u64, String)> {
    let admin = principal(test, "MT_ORG_A", Some(SCOPE_ALL_ORGS))?;
    let Some(dataset_ext_id) = env_var("MT_DATASET_EXT_ID") else {
        eprintln!("SKIP {test}: MT_DATASET_EXT_ID not configured");
        return None;
    };
    let dataset_id = acl_dataset_id(&admin, &dataset_ext_id).await;
    Some((admin, dataset_id, dataset_ext_id))
}

/// A read grant reads and does not write. Read and write are independent grants server-side, and
/// this is the direction people assume is safe.
#[tokio::test]
#[ignore]
async fn acl_read_only_grant_reads_but_cannot_write() -> Result<(), ResponseError> {
    const TEST: &str = "acl_read_only_grant_reads_but_cannot_write";
    let (Some(reader), Some((admin, dataset_id, _))) = (
        principal(TEST, "MT_READONLY", Some(SCOPE_ALL_ORGS)),
        acl_scenario(TEST).await,
    ) else {
        return Ok(());
    };

    let seeded = unique_id("acl_read");
    let mut guard = cleanup_resources_as(admin.config.clone(), vec![seeded.clone()]);
    admin
        .service
        .resources
        .create(vec![resource(&seeded, Some(dataset_id))], vec![])
        .await?;

    let read = reader.service.resources.by_ids(&by_external_id(&seeded)).await;
    assert!(
        !is_absent(&read),
        "{} holds /datasets/<ds>/read and must see the seeded resource — got {read:?}",
        reader.label
    );

    let denied = unique_id("acl_read_write_attempt");
    let error = assert_status(
        reader
            .service
            .resources
            .create(vec![resource(&denied, Some(dataset_id))], vec![])
            .await,
        403,
        "a read-only grant writing into its dataset",
    );
    let body = error.get_message();
    assert!(
        body.contains("\"permission\":\"write\"") || body.contains("write"),
        "the problem+json body should name the attempted permission — got: {body}"
    );

    admin
        .service
        .resources
        .delete(&by_external_id(&seeded))
        .await?;
    guard.disarm();
    Ok(())
}

/// A write grant writes and does not read — the direction that surprises people.
///
/// The read denial is probed through `fetch-related`, which authorises its **starting node** as a
/// single-item read. `byids` is a bulk endpoint and bulk reads are narrowed rather than refused,
/// so it would answer 200-with-nothing and prove less.
#[tokio::test]
#[ignore]
async fn acl_write_only_grant_writes_but_cannot_read() -> Result<(), ResponseError> {
    const TEST: &str = "acl_write_only_grant_writes_but_cannot_read";
    let (Some(writer), Some((admin, dataset_id, _))) = (
        principal(TEST, "MT_WRITEONLY", Some(SCOPE_ALL_ORGS)),
        acl_scenario(TEST).await,
    ) else {
        return Ok(());
    };

    let written = unique_id("acl_write");
    let mut guard = cleanup_resources_as(admin.config.clone(), vec![written.clone()]);
    writer
        .service
        .resources
        .create(vec![resource(&written, Some(dataset_id))], vec![])
        .await?;

    let error = assert_status(
        writer
            .service
            .resources
            .fetch_related(&RelatedResourcesForm::from_external_id(&written))
            .await,
        403,
        "a write-only grant reading the resource it just created",
    );
    assert!(
        error.get_message().contains("read"),
        "the problem+json body should name the attempted permission — got: {}",
        error.get_message()
    );

    // Teardown goes through the admin: the writer cannot read its own row back.
    admin
        .service
        .resources
        .delete(&by_external_id(&written))
        .await?;
    guard.disarm();
    Ok(())
}

/// No grant at all: denied in both directions.
#[tokio::test]
#[ignore]
async fn acl_principal_without_a_grant_is_denied_both_ways() -> Result<(), ResponseError> {
    const TEST: &str = "acl_principal_without_a_grant_is_denied_both_ways";
    let (Some(outsider), Some((admin, dataset_id, _))) = (
        principal(TEST, "MT_NOGRANT", Some(SCOPE_ALL_ORGS)),
        acl_scenario(TEST).await,
    ) else {
        return Ok(());
    };

    let seeded = unique_id("acl_nogrant");
    let mut guard = cleanup_resources_as(admin.config.clone(), vec![seeded.clone()]);
    admin
        .service
        .resources
        .create(vec![resource(&seeded, Some(dataset_id))], vec![])
        .await?;

    assert_status(
        outsider
            .service
            .resources
            .fetch_related(&RelatedResourcesForm::from_external_id(&seeded))
            .await,
        403,
        "an ungranted principal reading",
    );
    assert_status(
        outsider
            .service
            .resources
            .create(
                vec![resource(&unique_id("acl_nogrant_write"), Some(dataset_id))],
                vec![],
            )
            .await,
        403,
        "an ungranted principal writing",
    );

    admin
        .service
        .resources
        .delete(&by_external_id(&seeded))
        .await?;
    guard.disarm();
    Ok(())
}

/// Lists and searches omit what the caller may not read — 200, not 403.
///
/// The behaviour most likely to be assumed wrong: code that treats "no error" as "saw everything"
/// silently under-reports for a partially-granted caller, and nothing in the response says so.
#[tokio::test]
#[ignore]
async fn acl_list_and_search_omit_rows_rather_than_denying() -> Result<(), ResponseError> {
    const TEST: &str = "acl_list_and_search_omit_rows_rather_than_denying";
    let (Some(outsider), Some((admin, dataset_id, _))) = (
        principal(TEST, "MT_NOGRANT", Some(SCOPE_ALL_ORGS)),
        acl_scenario(TEST).await,
    ) else {
        return Ok(());
    };

    let seeded = unique_id("acl_narrowed");
    let mut guard = cleanup_resources_as(admin.config.clone(), vec![seeded.clone()]);
    admin
        .service
        .resources
        .create(vec![resource(&seeded, Some(dataset_id))], vec![])
        .await?;

    let mut search = SearchAndFilterForm::new();
    let mut form = SearchForm::new();
    form.query = Some(seeded.clone());
    search.search = Some(form);

    // Control: the admin does find it, so an empty result below means "narrowed", not "not there".
    let seen_by_admin = admin.service.resources.search(&search).await?;
    assert!(
        seen_by_admin
            .get_items()
            .iter()
            .any(|r| r.external_id == seeded),
        "the seeding admin should find its own resource"
    );

    let seen_by_outsider = outsider.service.resources.search(&search).await?;
    assert!(
        !seen_by_outsider
            .get_items()
            .iter()
            .any(|r| r.external_id == seeded),
        "an ungranted caller's search must omit the row"
    );

    admin
        .service
        .resources
        .delete(&by_external_id(&seeded))
        .await?;
    guard.disarm();
    Ok(())
}

/// Creating a dataset needs a blanket write role; a per-dataset grant is not enough.
#[tokio::test]
#[ignore]
async fn acl_dataset_management_requires_a_blanket_write_grant() -> Result<(), ResponseError> {
    const TEST: &str = "acl_dataset_management_requires_a_blanket_write_grant";
    let Some(writer) = principal(TEST, "MT_WRITEONLY", Some(SCOPE_ALL_ORGS)) else {
        return Ok(());
    };

    let dataset = Dataset::new(unique_id("acl_ds"));
    let guard = cleanup_datasets_as(writer.config.clone(), vec![dataset.external_id.clone()]);

    let error = assert_status(
        writer.service.datasets.create(&dataset).await,
        403,
        "a per-dataset write grant creating a dataset",
    );
    assert!(
        error.get_message().contains("manage") || error.get_message().contains("all-datasets"),
        "the denial should say a blanket grant is required — got: {}",
        error.get_message()
    );
    drop(guard);
    Ok(())
}

/// A grant on a parent dataset reaches its children through the `BELONGS_TO` hierarchy.
///
/// The child is linked with `connectedDataSets`, which the backend reads as "the dataset this one
/// is part of" and stores as the `BELONGS_TO` edge the ACL closure walks. Creating the dataset and
/// the edge bumps the ACL cache generation, so no wait for the ~10s permission cache is needed.
#[tokio::test]
#[ignore]
async fn acl_a_parent_dataset_grant_covers_descendants() -> Result<(), ResponseError> {
    const TEST: &str = "acl_a_parent_dataset_grant_covers_descendants";
    let (Some(reader), Some((admin, parent_id, _))) = (
        principal(TEST, "MT_READONLY", Some(SCOPE_ALL_ORGS)),
        acl_scenario(TEST).await,
    ) else {
        return Ok(());
    };

    let mut child = Dataset::new(unique_id("acl_child_ds"));
    child.add_connected_data_set(parent_id);
    let child_ext_id = child.external_id.clone();
    let mut ds_guard = cleanup_datasets_as(admin.config.clone(), vec![child_ext_id.clone()]);

    let created = admin.service.datasets.create(&child).await?;
    let child_id = created
        .get_items()
        .first()
        .and_then(|d| d.id)
        .expect("the created child dataset should come back with an id");

    let seeded = unique_id("acl_descendant");
    let mut res_guard = cleanup_resources_as(admin.config.clone(), vec![seeded.clone()]);
    admin
        .service
        .resources
        .create(vec![resource(&seeded, Some(child_id))], vec![])
        .await?;

    let read = reader.service.resources.by_ids(&by_external_id(&seeded)).await;
    assert!(
        !is_absent(&read),
        "a read grant on the parent dataset should reach a resource in the child — got {read:?}"
    );

    admin
        .service
        .resources
        .delete(&by_external_id(&seeded))
        .await?;
    res_guard.disarm();
    admin
        .service
        .datasets
        .delete(&by_external_id(&child_ext_id))
        .await?;
    ds_guard.disarm();
    Ok(())
}

/// An entity in no dataset is reachable only with an all-datasets grant.
///
/// There is no dataset to match a per-dataset grant against, so the ACL fails closed rather than
/// treating "no dataset" as "no restriction".
#[tokio::test]
#[ignore]
async fn acl_orphan_entities_need_a_blanket_grant() -> Result<(), ResponseError> {
    const TEST: &str = "acl_orphan_entities_need_a_blanket_grant";
    let (Some(reader), Some(admin)) = (
        principal(TEST, "MT_READONLY", Some(SCOPE_ALL_ORGS)),
        principal(TEST, "MT_ORG_A", Some(SCOPE_ALL_ORGS)),
    ) else {
        return Ok(());
    };

    let orphan = unique_id("acl_orphan");
    let mut guard = cleanup_resources_as(admin.config.clone(), vec![orphan.clone()]);
    admin
        .service
        .resources
        .create(vec![resource(&orphan, None)], vec![])
        .await?;

    assert_status(
        reader
            .service
            .resources
            .fetch_related(&RelatedResourcesForm::from_external_id(&orphan))
            .await,
        403,
        "a per-dataset read grant reading an entity that has no dataset",
    );

    admin
        .service
        .resources
        .delete(&by_external_id(&orphan))
        .await?;
    guard.disarm();
    Ok(())
}

// ---------------------------------------------------------------------------------------------
// How an ACL denial interacts with durable ingest buffering
// ---------------------------------------------------------------------------------------------

/// An ACL-denied datapoint send is **spooled to disk, not surfaced** — today's behaviour, pinned.
///
/// [`ResponseError::is_bufferable`] counts 401/403 as recoverable, on the theory that a credential
/// can be fixed out of band and the data shouldn't be dropped meanwhile. That is right for an
/// expired token and wrong for a dataset ACL: the grant is not going to appear on its own, so the
/// spool grows and the caller is told nothing. This test exists so that if the policy is ever
/// narrowed — say, buffering 401 but surfacing 403 — the change is deliberate and visible here.
#[tokio::test]
#[ignore]
async fn acl_a_denied_datapoint_send_is_spooled_not_surfaced() -> Result<(), ResponseError> {
    const TEST: &str = "acl_a_denied_datapoint_send_is_spooled_not_surfaced";
    let (Some(reader), Some((admin, dataset_id, _))) = (
        principal(TEST, "MT_READONLY", Some(SCOPE_ALL_ORGS)),
        acl_scenario(TEST).await,
    ) else {
        return Ok(());
    };

    let series_ext_id = unique_id("acl_dp");
    let mut guard = cleanup_timeseries_as(admin.config.clone(), vec![series_ext_id.clone()]);
    let mut series = TimeSeries::new(&series_ext_id, &series_ext_id);
    series.data_set_id = Some(dataset_id);
    let mut payload: DataWrapper<TimeSeries> = DataWrapper::new();
    payload.add_item(series);
    admin.service.time_series.create(&payload).await?;

    let dir = crate::buffer_integration::temp_dir();
    let buffered_reader = reader.buffered(dir.clone());

    let sent = buffered_reader
        .service
        .time_series
        .insert_datapoint(
            None,
            Some(series_ext_id.clone()),
            Utc::now(),
            "1.0".to_string(),
        )
        .await;
    assert!(
        sent.is_ok(),
        "a 403 on ingest is classified as bufferable, so the send reports success: {sent:?}"
    );
    assert!(
        buffered_reader.service.time_series.buffered_count() > 0,
        "the denied datapoint should be sitting in the on-disk spool at {}",
        dir.display()
    );

    admin
        .service
        .time_series
        .delete(&DataWrapper::from_vec(by_external_id(&series_ext_id)))
        .await?;
    guard.disarm();
    std::fs::remove_dir_all(&dir).ok();
    Ok(())
}
