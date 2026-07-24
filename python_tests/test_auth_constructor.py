"""The explicit-kwargs ("naive") constructor must wire up the same auth paths as
``from_envfile``. Covered modes, each skipped when the ``.env`` lacks its inputs:

- **client_credentials** — ``client_id`` + ``client_secret`` + ``token_url``, no
  assertion kwargs (an assertion source would switch the grant to jwt-bearer).
  Reads ``TEST_CC_CLIENT_ID``/``TEST_CC_CLIENT_SECRET`` when set, falling back to
  ``CLIENT_ID``/``CLIENT_SECRET`` — so a federated ``.env`` (which deliberately
  has no ``CLIENT_SECRET``) can still exercise this mode via a dedicated
  secret-auth test client.
- **federated jwt-bearer** — an ``assertion_*`` source and *no* client secret,
  so the exchange authenticates with the assertion itself (RFC 7523
  ``client_assertion``).

Values are fed through constructor kwargs explicitly — never ``from_envfile`` —
so a regression that only breaks argument forwarding can't hide behind the
env-loading path the rest of the suite uses.
"""
import os

import datahub_sdk
import pytest

from fixtures import ENV_FILE


def _read_env(path):
    """Minimal KEY=VALUE parser — enough for the flat .env this suite uses."""
    values = {}
    with open(path) as fh:
        for line in fh:
            line = line.strip()
            if not line or line.startswith("#") or "=" not in line:
                continue
            key, _, value = line.partition("=")
            values[key.strip()] = value.strip().strip('"').strip("'")
    return values


@pytest.fixture(scope="module")
def env():
    if not os.path.exists(ENV_FILE):
        pytest.skip(".env not present")
    values = _read_env(ENV_FILE)
    if "BASE_URL" not in values:
        pytest.skip("BASE_URL not configured")
    return values


def _client_credentials_kwargs(env):
    client_id = env.get("TEST_CC_CLIENT_ID") or env.get("CLIENT_ID")
    client_secret = env.get("TEST_CC_CLIENT_SECRET") or env.get("CLIENT_SECRET")
    if not (env.get("TOKEN_URI") and client_id and client_secret):
        pytest.skip("no client_credentials config (TOKEN_URI + [TEST_CC_]CLIENT_ID/SECRET)")
    return dict(
        token_url=env["TOKEN_URI"],
        client_id=client_id,
        client_secret=client_secret,
        scope=env.get("SCOPE"),
        audience=env.get("AUDIENCE"),
        project_name=env.get("PROJECT_NAME"),
    )


def _federated_kwargs(env):
    has_source = env.get("ASSERTION") or (
        env.get("ASSERTION_TOKEN_URI")
        and env.get("ASSERTION_CLIENT_ID")
        and env.get("ASSERTION_CLIENT_SECRET")
    )
    if not (env.get("TOKEN_URI") and has_source):
        pytest.skip("no federated config (TOKEN_URI + ASSERTION or ASSERTION_* triple)")
    # Deliberately no client_id/client_secret: a secret would flip the exchange
    # back to basic auth, and a client_id is resolved from the assertion.
    return dict(
        token_url=env["TOKEN_URI"],
        scope=env.get("SCOPE"),
        audience=env.get("AUDIENCE"),
        project_name=env.get("PROJECT_NAME"),
        assertion=env.get("ASSERTION"),
        assertion_token_url=env.get("ASSERTION_TOKEN_URI"),
        assertion_client_id=env.get("ASSERTION_CLIENT_ID"),
        assertion_client_secret=env.get("ASSERTION_CLIENT_SECRET"),
        assertion_scope=env.get("ASSERTION_SCOPE"),
        assertion_audience=env.get("ASSERTION_AUDIENCE"),
        assertion_grant=env.get("ASSERTION_GRANT"),
    )


def _assert_lists_units_sync(base_url, kwargs):
    client = datahub_sdk.DataHubClient(base_url, **kwargs)
    units = client.units.list()
    assert isinstance(units, list)
    assert len(units) > 0


async def _assert_lists_units_async(base_url, kwargs):
    client = datahub_sdk.AsyncDataHubClient(base_url, **kwargs)
    units = await client.units.list()
    assert isinstance(units, list)
    assert len(units) > 0


def test_sync_client_credentials(env):
    _assert_lists_units_sync(env["BASE_URL"], _client_credentials_kwargs(env))


@pytest.mark.asyncio
async def test_async_client_credentials(env):
    await _assert_lists_units_async(env["BASE_URL"], _client_credentials_kwargs(env))


def test_sync_federated_jwt(env):
    _assert_lists_units_sync(env["BASE_URL"], _federated_kwargs(env))


@pytest.mark.asyncio
async def test_async_federated_jwt(env):
    await _assert_lists_units_async(env["BASE_URL"], _federated_kwargs(env))
