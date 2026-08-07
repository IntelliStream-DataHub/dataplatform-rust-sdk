//! Explaining a 401 that the API declines to explain.
//!
//! DataHub resolves the caller's tenant from the access token's `organization` claim, and rejects
//! the token outright when that claim is missing, malformed, or names more than one organization.
//! The reason never reaches the caller: the API installs a custom authentication entry point that
//! answers with a bare `WWW-Authenticate: Bearer realm="Restricted Content"` and a generic body,
//! logging the real reason server-side only. What arrives here is
//! `ResponseError { status: 401, message: "" }` — indistinguishable from a rotated secret, which
//! is why the usual first response is to go and rotate the secret.
//!
//! The token itself carries the answer, so the SDK reconstructs it locally: decode the payload,
//! look at `organization`, and report which case the server would have hit.
//!
//! # This discloses nothing
//!
//! Only the caller's *own* token is inspected — one the SDK already holds in memory — and a JWT
//! payload is base64, not encrypted, so its holder can read it at any time (`cut -d. -f2 |
//! base64 -d`). Nothing crosses the network, no signature is verified, and no server-side state is
//! consulted. This is diagnosis, never an access decision: the API remains the only thing that
//! decides whether a token is acceptable, and a token this module considers well-formed can still
//! be rejected for reasons it cannot see (expiry, revocation, audience, signature).

use base64::engine::general_purpose::URL_SAFE_NO_PAD;
use base64::Engine;

/// What the `organization` claim of a token looks like, in the terms the API's
/// `OrganizationValidator` reasons about.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) enum OrganizationClaim {
    /// Not a readable JWT — an opaque token supplied through `TOKEN`, say. Nothing to say about it.
    NotAJwt,
    /// A JWT, but with no `organization` claim: the token request asked for no organization scope,
    /// or the realm emits the claim only under a selector.
    Absent,
    /// Present but empty (`{}`), which the API treats the same as absent.
    Empty,
    /// A JSON array rather than an object. Keycloak merges same-named multivalued claims, so this
    /// means two mappers are writing `organization`, or the membership mapper has
    /// `addOrganizationId` switched off and is emitting bare aliases.
    Array,
    /// Exactly one organization — the shape the API accepts.
    One,
    /// Several organizations. The API refuses to guess which tenant is meant.
    Many(Vec<String>),
}

/// Inspect the `organization` claim of `token`. Best-effort and infallible: anything that does not
/// parse is [`OrganizationClaim::NotAJwt`], because the alternative — guessing — would blame the
/// wrong thing for an opaque token.
pub(crate) fn inspect_organization_claim(token: &str) -> OrganizationClaim {
    // A JWS is `header.payload.signature`; we want the middle segment and never the signature.
    let Some(payload) = token.split('.').nth(1).filter(|_| token.split('.').count() == 3) else {
        return OrganizationClaim::NotAJwt;
    };
    let Ok(bytes) = URL_SAFE_NO_PAD.decode(payload) else {
        return OrganizationClaim::NotAJwt;
    };
    let Ok(claims) = serde_json::from_slice::<serde_json::Value>(&bytes) else {
        return OrganizationClaim::NotAJwt;
    };
    if !claims.is_object() {
        return OrganizationClaim::NotAJwt;
    }

    match claims.get("organization") {
        None | Some(serde_json::Value::Null) => OrganizationClaim::Absent,
        Some(serde_json::Value::Array(_)) => OrganizationClaim::Array,
        Some(serde_json::Value::Object(orgs)) if orgs.is_empty() => OrganizationClaim::Empty,
        Some(serde_json::Value::Object(orgs)) if orgs.len() == 1 => OrganizationClaim::One,
        Some(serde_json::Value::Object(orgs)) => {
            // Sorted: a JSON object's key order is not meaningful (and depends on whether
            // serde_json is built with `preserve_order`), while an error message that reshuffles
            // between runs is hard to grep for and hard to assert on.
            let mut aliases: Vec<String> = orgs.keys().cloned().collect();
            aliases.sort();
            OrganizationClaim::Many(aliases)
        }
        // Any other JSON type is malformed in a way the API also rejects, and "array" is the
        // closest actionable advice we have.
        Some(_) => OrganizationClaim::Array,
    }
}

/// A sentence explaining why this token's tenant could not be resolved, or `None` when the claim is
/// well-formed and the 401 must have another cause.
pub(crate) fn organization_hint(token: &str) -> Option<String> {
    let advice = match inspect_organization_claim(token) {
        OrganizationClaim::NotAJwt | OrganizationClaim::One => return None,
        OrganizationClaim::Absent => "the access token carries no `organization` claim, so the \
             server cannot tell which tenant you mean. DataHub resolves the tenant from that \
             claim; set the OAuth2 scope (`SCOPE=organization:*`, or `organization:<alias>` to \
             pin one) if your realm emits it under a selector."
            .to_string(),
        OrganizationClaim::Empty => "the access token's `organization` claim is empty, so the \
             server cannot tell which tenant you mean. Check that this principal is a member of \
             an organization, and that the token request asks for the organization scope."
            .to_string(),
        OrganizationClaim::Array => "the access token's `organization` claim is a JSON array, \
             which the server rejects. Two protocol mappers are writing that claim, or the \
             organization membership mapper has `addOrganizationId` switched off."
            .to_string(),
        OrganizationClaim::Many(aliases) => {
            format!(
                "the access token names {} organizations ({}), and the server will not choose one \
                 for you. Pin a single tenant with `SCOPE=organization:<alias>`.",
                aliases.len(),
                aliases.join(", ")
            )
        }
    };
    Some(format!(
        "{advice} (Diagnosed by the SDK from your own token; the server does not report this.)"
    ))
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    /// A token whose payload is `claims`. The header and signature are junk on purpose — nothing
    /// here verifies them, and a test that supplied a real signature would imply otherwise.
    fn jwt(claims: serde_json::Value) -> String {
        format!(
            "aGVhZGVy.{}.c2ln",
            URL_SAFE_NO_PAD.encode(claims.to_string())
        )
    }

    #[test]
    fn a_single_organization_is_well_formed_and_gets_no_hint() {
        let token = jwt(json!({"organization": {"acme": {"id": "abc"}}}));
        assert_eq!(inspect_organization_claim(&token), OrganizationClaim::One);
        // The token is fine, so whatever caused the 401 is something this module can't see —
        // inventing an explanation would send the reader down the wrong path.
        assert_eq!(organization_hint(&token), None);
    }

    #[test]
    fn several_organizations_are_named_in_the_message() {
        let token = jwt(json!({"organization": {
            "beta": {"id": "2"},
            "acme": {"id": "1"},
        }}));
        // Sorted, not in the order they appeared in the claim.
        assert_eq!(
            inspect_organization_claim(&token),
            OrganizationClaim::Many(vec!["acme".into(), "beta".into()])
        );

        let hint = organization_hint(&token).expect("a two-organization token should be explained");
        assert!(hint.contains("names 2 organizations"), "{hint}");
        // Sorted, so the message is stable across runs rather than following JSON key order.
        assert!(hint.contains("(acme, beta)"), "{hint}");
        // The actionable part: what to actually change.
        assert!(hint.contains("SCOPE=organization:<alias>"), "{hint}");
    }

    #[test]
    fn a_missing_claim_points_at_the_scope() {
        let token = jwt(json!({"sub": "service-account-x"}));
        assert_eq!(inspect_organization_claim(&token), OrganizationClaim::Absent);
        let hint = organization_hint(&token).unwrap();
        assert!(hint.contains("no `organization` claim"), "{hint}");
        assert!(hint.contains("SCOPE=organization:*"), "{hint}");
    }

    #[test]
    fn a_null_claim_is_treated_as_absent() {
        // Keycloak emits `"organization": null` for a principal with no resolvable membership.
        let token = jwt(json!({"organization": null}));
        assert_eq!(inspect_organization_claim(&token), OrganizationClaim::Absent);
        assert!(organization_hint(&token).is_some());
    }

    #[test]
    fn an_empty_claim_is_reported_separately_from_a_missing_one() {
        let token = jwt(json!({"organization": {}}));
        assert_eq!(inspect_organization_claim(&token), OrganizationClaim::Empty);
        let hint = organization_hint(&token).unwrap();
        assert!(hint.contains("is empty"), "{hint}");
    }

    #[test]
    fn an_array_claim_points_at_the_mapper_configuration() {
        // What two mappers writing the same claim produces — the shape that cost this project a
        // day: one mapper emitting bare aliases, another the nested object, merged by Keycloak.
        let token = jwt(json!({"organization": ["acme", {"acme": {"id": "1"}}]}));
        assert_eq!(inspect_organization_claim(&token), OrganizationClaim::Array);
        let hint = organization_hint(&token).unwrap();
        assert!(hint.contains("JSON array"), "{hint}");
        assert!(hint.contains("addOrganizationId"), "{hint}");
    }

    #[test]
    fn a_flat_alias_array_is_also_reported_as_an_array() {
        // `addOrganizationId` off, single mapper: `["acme"]`. Same rejection, same advice.
        let token = jwt(json!({"organization": ["acme"]}));
        assert_eq!(inspect_organization_claim(&token), OrganizationClaim::Array);
    }

    #[test]
    fn an_opaque_token_is_never_speculated_about() {
        // A user-supplied `TOKEN=` need not be a JWT. Reporting "you have no organization claim"
        // for one would blame the wrong thing entirely.
        for opaque in ["", "not-a-jwt", "two.parts", "a.b.c.d", "aGVhZGVy.!!!.c2ln"] {
            assert_eq!(
                inspect_organization_claim(opaque),
                OrganizationClaim::NotAJwt,
                "{opaque:?} should not be treated as a JWT"
            );
            assert_eq!(organization_hint(opaque), None, "{opaque:?}");
        }
    }

    #[test]
    fn a_payload_that_is_not_a_json_object_is_not_a_jwt() {
        let token = format!("aGVhZGVy.{}.c2ln", URL_SAFE_NO_PAD.encode("[1,2,3]"));
        assert_eq!(inspect_organization_claim(&token), OrganizationClaim::NotAJwt);
    }
}
