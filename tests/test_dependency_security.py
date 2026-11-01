"""Runtime TLS trust follows the corrected certifi release boundary."""
import hashlib
import ssl
from pathlib import Path

import certifi
from packaging.requirements import Requirement
from packaging.version import Version
try:
    import tomllib
except ModuleNotFoundError:
    import tomli as tomllib


def test_runtime_certificate_requirement_excludes_the_affected_bundle():
    """The declared runtime floor excludes the affected release."""
    manifest = tomllib.loads((Path(__file__).parents[1] / "pyproject.toml").read_text())
    requirement = next(Requirement(item) for item in manifest["project"]["dependencies"]
                       if Requirement(item).name == "certifi")
    assert Version("2026.4.22") not in requirement.specifier
    assert Version("2026.10.19") in requirement.specifier
    assert Version("2026.10.23") in requirement.specifier


def test_actual_certificate_bundle_removes_compromised_root_and_keeps_verification():
    """Installed trust excludes the affected issuer without disabling TLS checks."""
    context = ssl.create_default_context(cafile=certifi.where())
    roots = context.get_ca_certs(binary_form=True)
    fingerprints = {hashlib.sha256(der).hexdigest() for der in roots}
    assert "86a1ecba089c4a8d3bbe2734c612ba341d813e043cf9e8a862cd5c57a36bbe6b" not in fingerprints
    assert len(roots) > 100
    assert context.check_hostname is True
    assert context.verify_mode == ssl.CERT_REQUIRED


def test_developer_lint_requirement_excludes_pickle_cache_releases():
    """The developer lint floor excludes the vulnerable pickle cache loader."""
    manifest = tomllib.loads((Path(__file__).parents[1] / "pyproject.toml").read_text())
    requirement = next(
        Requirement(item) for item in manifest["project"]["optional-dependencies"]["dev"]
        if Requirement(item).name == "pylint"
    )
    assert Version("4.0.5") not in requirement.specifier
    assert Version("4.0.9") in requirement.specifier
    assert Version("4.0.10") in requirement.specifier
