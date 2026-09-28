"""Tests for the `/tenant` bindings."""
import intellistream_datahub_sdk
import pytest

from fixtures import async_client, sync_client


def test_features_answer_every_flag(sync_client):
    features = sync_client.tenant.features()
    for flag in ("files", "policy", "streaming", "chat"):
        assert isinstance(getattr(features, flag), bool)


def test_llm_settings_follow_the_reported_permission(sync_client):
    permissions = sync_client.tenant.settings_permissions()
    assert "llm" in permissions
    if permissions["llm"].read:
        settings = sync_client.tenant.llm_settings()
        assert isinstance(settings.configured, bool)
    else:
        with pytest.raises(intellistream_datahub_sdk.DataHubException) as err:
            sync_client.tenant.llm_settings()
        assert err.value.status_code == 403


@pytest.mark.asyncio
async def test_async_features(async_client):
    features = await async_client.tenant.features()
    assert isinstance(features.files, bool)


def test_update_llm_settings_is_gated_validated_and_round_trips(sync_client):
    """Mirrors the Rust test: 403 without the write grant; with it, an empty form is a 400 naming
    `provider` and `model`, and a configured model written back unchanged answers what was stored.
    The grant is checked before the form, so no branch changes the tenant's settings."""
    permissions = sync_client.tenant.settings_permissions()
    if not permissions["llm"].write:
        with pytest.raises(intellistream_datahub_sdk.DataHubException) as err:
            sync_client.tenant.update_llm_settings()
        assert err.value.status_code == 403
        return

    with pytest.raises(intellistream_datahub_sdk.DataHubException) as err:
        sync_client.tenant.update_llm_settings()
    assert err.value.status_code == 400
    fields = {f.get("field") for f in (err.value.problem or {}).get("fields", [])}
    assert {"provider", "model"} <= fields

    stored = sync_client.tenant.llm_settings()
    if not stored.configured:
        pytest.skip("no model configured; nothing to write back unchanged")
    saved = sync_client.tenant.update_llm_settings(
        provider=stored.provider,
        model=stored.model,
        base_url=stored.base_url,
        reasoning_effort=stored.reasoning_effort,
        effort=stored.effort,
        turn_timeout=stored.turn_timeout,
        max_output_tokens=stored.max_output_tokens,
        max_iterations=stored.max_iterations,
        instructions=stored.instructions,
    )
    for attr in ("provider", "model", "base_url", "effort", "instructions", "api_key_set"):
        assert getattr(saved, attr) == getattr(stored, attr), attr
