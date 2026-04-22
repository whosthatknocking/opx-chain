"""Tests for installed-version resolution helpers."""

from importlib.metadata import PackageNotFoundError

from opx_chain import version as version_module


def test_get_version_reads_the_current_package_distribution(monkeypatch):
    """Installed-version lookup must target the opx-chain distribution name."""
    captured = {}

    def stub_version(dist_name: str) -> str:
        captured["dist_name"] = dist_name
        return "9.9.9"

    monkeypatch.setattr(version_module, "version", stub_version)

    assert version_module.get_version() == "9.9.9"
    assert captured == {"dist_name": "opx-chain"}


def test_get_version_falls_back_to_pyproject_when_distribution_missing(monkeypatch):
    """Source checkouts should still report the pyproject version when not installed."""
    monkeypatch.setattr(
        version_module,
        "version",
        lambda _dist_name: (_ for _ in ()).throw(PackageNotFoundError()),
    )
    monkeypatch.setattr(version_module, "_version_from_pyproject", lambda: "0.2.0")

    assert version_module.get_version() == "0.2.0"
