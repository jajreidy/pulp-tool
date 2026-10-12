"""Tests for RPM GPG verification helper."""

import logging
from pathlib import Path
from unittest.mock import MagicMock, patch

import pytest

from pulp_tool.utils.rpm_gpg_verify import (
    RpmGpgVerifyError,
    _checksig_indicates_valid_signature,
    _ephemeral_rpmdb_for_gnupg_home,
    _export_pubkey_armor,
    _rpmkeys_import_argv,
    _run_rpm_checksig,
    verify_pulled_rpms_gpg,
    verify_rpm_gpg_signature,
)

_VALID_CHECKSIG_OUTPUT = (
    "pkg.rpm:\n    Header V4 RSA/SHA256 Signature, key ID 01234567: OK\n    Header SHA256 digest: OK\n"
)


class TestRpmGpgVerify:
    """Tests for verify_rpm_gpg_signature."""

    def test_missing_rpm_file(self, tmp_path: Path) -> None:
        with pytest.raises(RpmGpgVerifyError, match="RPM not found"):
            verify_rpm_gpg_signature(tmp_path / "missing.rpm")

    def test_missing_keyring_path(self, tmp_path: Path) -> None:
        rpm_path = tmp_path / "pkg.rpm"
        rpm_path.write_bytes(b"fake")
        with pytest.raises(RpmGpgVerifyError, match="keyring path does not exist"):
            verify_rpm_gpg_signature(rpm_path, gpg_keyring=str(tmp_path / "no-such-dir"))

    def test_rpm_cli_missing(self, tmp_path: Path) -> None:
        rpm_path = tmp_path / "pkg.rpm"
        rpm_path.write_bytes(b"fake")
        with patch("pulp_tool.utils.rpm_gpg_verify.subprocess.run", side_effect=FileNotFoundError):
            with pytest.raises(RpmGpgVerifyError, match="rpm CLI not found"):
                verify_rpm_gpg_signature(rpm_path)

    def test_rpm_checksig_failure(self, tmp_path: Path) -> None:
        rpm_path = tmp_path / "pkg.rpm"
        rpm_path.write_bytes(b"fake")
        mock_result = MagicMock(returncode=1, stdout="", stderr="signature NOT OK")
        with patch("pulp_tool.utils.rpm_gpg_verify.subprocess.run", return_value=mock_result):
            with pytest.raises(RpmGpgVerifyError, match="rpm --checksig failed"):
                verify_rpm_gpg_signature(rpm_path)

    def test_checksig_indicates_valid_signature(self) -> None:
        assert _checksig_indicates_valid_signature(_VALID_CHECKSIG_OUTPUT)
        assert not _checksig_indicates_valid_signature("pkg.rpm: digests signatures OK\n")
        assert not _checksig_indicates_valid_signature("pkg.rpm:\n    RSA Signature, key ID abc: NOKEY\n")

    def test_rpm_checksig_success(self, tmp_path: Path) -> None:
        rpm_path = tmp_path / "pkg.rpm"
        rpm_path.write_bytes(b"fake")
        mock_result = MagicMock(returncode=0, stdout=_VALID_CHECKSIG_OUTPUT, stderr="")
        with patch("pulp_tool.utils.rpm_gpg_verify.subprocess.run", return_value=mock_result) as mock_run:
            verify_rpm_gpg_signature(rpm_path)
            mock_run.assert_called_once()
            assert mock_run.call_args.kwargs.get("env") is not None

    def test_rpm_checksig_imports_keys_when_keyring_provided(self, tmp_path: Path) -> None:
        keyring = tmp_path / "gpg-home"
        keyring.mkdir()
        rpm_path = tmp_path / "pkg.rpm"
        rpm_path.write_bytes(b"fake")
        rpmdb = tmp_path / "rpmdb"
        rpmdb.mkdir()
        ok = MagicMock(returncode=0, stdout=_VALID_CHECKSIG_OUTPUT, stderr="")
        with (
            patch(
                "pulp_tool.utils.rpm_gpg_verify._ephemeral_rpmdb_for_gnupg_home",
                return_value=_fake_cm(rpmdb),
            ),
            patch("pulp_tool.utils.rpm_gpg_verify.subprocess.run", return_value=ok) as mock_run,
        ):
            verify_rpm_gpg_signature(rpm_path, gpg_keyring=str(keyring))
            cmd = mock_run.call_args.args[0]
            assert cmd[0] == "rpm"
            assert "--checksig" in cmd
            assert "--verbose" in cmd
            assert "_pkgverify_level signature" in " ".join(cmd)
            assert f"_dbpath {rpmdb}" in " ".join(cmd)
            assert mock_run.call_args.kwargs["env"]["GNUPGHOME"] == str(keyring.resolve())

    def test_rpm_checksig_reports_not_ok_in_output(self, tmp_path: Path) -> None:
        rpm_path = tmp_path / "pkg.rpm"
        rpm_path.write_bytes(b"fake")
        mock_result = MagicMock(returncode=0, stdout="pkg.rpm: NOT OK\n", stderr="")
        with patch("pulp_tool.utils.rpm_gpg_verify.subprocess.run", return_value=mock_result):
            with pytest.raises(RpmGpgVerifyError, match="missing or invalid GPG signature"):
                verify_rpm_gpg_signature(rpm_path)

    def test_rpm_checksig_digest_only_output_fails(self, tmp_path: Path) -> None:
        rpm_path = tmp_path / "pkg.rpm"
        rpm_path.write_bytes(b"fake")
        mock_result = MagicMock(returncode=0, stdout="pkg.rpm: digests signatures OK\n", stderr="")
        with patch("pulp_tool.utils.rpm_gpg_verify.subprocess.run", return_value=mock_result):
            with pytest.raises(RpmGpgVerifyError, match="missing or invalid GPG signature"):
                verify_rpm_gpg_signature(rpm_path)

    def test_gpg_missing_when_keyring_provided(self, tmp_path: Path) -> None:
        keyring = tmp_path / "gpg-home"
        keyring.mkdir()
        rpm_path = tmp_path / "pkg.rpm"
        rpm_path.write_bytes(b"fake")
        with patch("pulp_tool.utils.rpm_gpg_verify.shutil.which", return_value=None):
            with pytest.raises(RpmGpgVerifyError, match="gpg CLI not found"):
                verify_rpm_gpg_signature(rpm_path, gpg_keyring=str(keyring))

    def test_verify_pulled_rpms_stops_on_first_error(self, tmp_path: Path) -> None:
        first = tmp_path / "a.rpm"
        second = tmp_path / "b.rpm"
        first.write_bytes(b"a")
        second.write_bytes(b"b")
        with patch(
            "pulp_tool.utils.rpm_gpg_verify.verify_rpm_gpg_signature",
            side_effect=[RpmGpgVerifyError("bad sig"), None],
        ) as mock_verify:
            with pytest.raises(RpmGpgVerifyError, match="bad sig"):
                verify_pulled_rpms_gpg([first, second], gpg_keyring=None)
            assert mock_verify.call_count == 1

    def test_rpmkeys_import_argv(self, tmp_path: Path) -> None:
        asc = tmp_path / "keys.asc"
        rpmdb = tmp_path / "rpmdb"
        assert _rpmkeys_import_argv(asc, rpmdb_path=rpmdb) == [
            "rpmkeys",
            "--define",
            f"_dbpath {rpmdb}",
            "--import",
            str(asc),
        ]

    def test_export_pubkey_uses_gpg2_when_gpg_missing(self, tmp_path: Path) -> None:
        gnupg_home = tmp_path / "gh"
        gnupg_home.mkdir()
        asc = tmp_path / "out.asc"

        def which(name: str) -> str | None:
            return "/usr/bin/gpg2" if name == "gpg2" else None

        def fake_run(cmd: list[str], **_kwargs: object) -> MagicMock:
            Path(cmd[cmd.index("-o") + 1]).write_text("PUBKEY")
            return MagicMock(returncode=0, stdout="", stderr="")

        with (
            patch("pulp_tool.utils.rpm_gpg_verify.shutil.which", side_effect=which),
            patch("pulp_tool.utils.rpm_gpg_verify.subprocess.run", side_effect=fake_run) as mock_run,
        ):
            _export_pubkey_armor(gnupg_home, asc)
            assert mock_run.call_args.args[0][0] == "/usr/bin/gpg2"

    def test_export_pubkey_gpg_export_nonzero(self, tmp_path: Path) -> None:
        gnupg_home = tmp_path / "gh"
        gnupg_home.mkdir()
        asc = tmp_path / "out.asc"
        bad = MagicMock(returncode=2, stdout="", stderr="export failed")
        with (
            patch("pulp_tool.utils.rpm_gpg_verify.shutil.which", return_value="/usr/bin/gpg"),
            patch("pulp_tool.utils.rpm_gpg_verify.subprocess.run", return_value=bad),
        ):
            with pytest.raises(RpmGpgVerifyError, match="gpg --export failed"):
                _export_pubkey_armor(gnupg_home, asc)

    def test_export_pubkey_no_keys_written(self, tmp_path: Path) -> None:
        gnupg_home = tmp_path / "gh"
        gnupg_home.mkdir()
        asc = tmp_path / "out.asc"
        ok = MagicMock(returncode=0, stdout="", stderr="")
        with (
            patch("pulp_tool.utils.rpm_gpg_verify.shutil.which", return_value="/usr/bin/gpg"),
            patch("pulp_tool.utils.rpm_gpg_verify.subprocess.run", return_value=ok),
        ):
            with pytest.raises(RpmGpgVerifyError, match="No public keys exported"):
                _export_pubkey_armor(gnupg_home, asc)

    def test_ephemeral_rpmdb_imports_keys(self, tmp_path: Path) -> None:
        gnupg_home = tmp_path / "gh"
        gnupg_home.mkdir()

        def fake_run(cmd: list[str], **_kwargs: object) -> MagicMock:
            if "--export" in cmd:
                out = Path(cmd[cmd.index("-o") + 1])
                out.write_text("PUBKEY")
                return MagicMock(returncode=0, stdout="", stderr="")
            if cmd[0] == "rpmkeys":
                assert "--import" in cmd
                return MagicMock(returncode=0, stdout="", stderr="")
            raise AssertionError(f"unexpected cmd: {cmd}")

        with (
            patch("pulp_tool.utils.rpm_gpg_verify.shutil.which", return_value="/usr/bin/gpg"),
            patch("pulp_tool.utils.rpm_gpg_verify.subprocess.run", side_effect=fake_run),
        ):
            with _ephemeral_rpmdb_for_gnupg_home(gnupg_home) as rpmdb_path:
                assert rpmdb_path.is_dir()

    def test_ephemeral_rpmdb_rpmkeys_missing(self, tmp_path: Path) -> None:
        gnupg_home = tmp_path / "gh"
        gnupg_home.mkdir()

        def fake_run(cmd: list[str], **_kwargs: object) -> MagicMock:
            if "--export" in cmd:
                out = Path(cmd[cmd.index("-o") + 1])
                out.write_text("PUBKEY")
                return MagicMock(returncode=0, stdout="", stderr="")
            if cmd[0] == "rpmkeys":
                raise FileNotFoundError("rpmkeys")
            raise AssertionError(cmd)

        with (
            patch("pulp_tool.utils.rpm_gpg_verify.shutil.which", return_value="/usr/bin/gpg"),
            patch("pulp_tool.utils.rpm_gpg_verify.subprocess.run", side_effect=fake_run),
        ):
            with pytest.raises(RpmGpgVerifyError, match="rpmkeys CLI not found"):
                with _ephemeral_rpmdb_for_gnupg_home(gnupg_home):
                    pass

    def test_ephemeral_rpmdb_import_fails(self, tmp_path: Path) -> None:
        gnupg_home = tmp_path / "gh"
        gnupg_home.mkdir()

        def fake_run(cmd: list[str], **_kwargs: object) -> MagicMock:
            if "--export" in cmd:
                out = Path(cmd[cmd.index("-o") + 1])
                out.write_text("PUBKEY")
                return MagicMock(returncode=0, stdout="", stderr="")
            if cmd[0] == "rpmkeys":
                return MagicMock(returncode=1, stdout="", stderr="bad import")
            raise AssertionError(cmd)

        with (
            patch("pulp_tool.utils.rpm_gpg_verify.shutil.which", return_value="/usr/bin/gpg"),
            patch("pulp_tool.utils.rpm_gpg_verify.subprocess.run", side_effect=fake_run),
        ):
            with pytest.raises(RpmGpgVerifyError, match="rpmkeys --import failed"):
                with _ephemeral_rpmdb_for_gnupg_home(gnupg_home):
                    pass

    def test_run_rpm_checksig_success_debug(self, tmp_path: Path, caplog: pytest.LogCaptureFixture) -> None:
        rpm_path = tmp_path / "pkg.rpm"
        rpm_path.write_bytes(b"fake")
        ok = MagicMock(returncode=0, stdout=_VALID_CHECKSIG_OUTPUT, stderr="")
        with (
            caplog.at_level(logging.DEBUG),
            patch("pulp_tool.utils.rpm_gpg_verify.subprocess.run", return_value=ok),
        ):
            _run_rpm_checksig(rpm_path, rpmdb_path=None, env={})
        assert "RPM GPG check OK" in caplog.text

    def test_verify_pulled_rpms_no_keyring_success(self, tmp_path: Path) -> None:
        rpm_path = tmp_path / "a.rpm"
        rpm_path.write_bytes(b"a")
        ok = MagicMock(returncode=0, stdout=_VALID_CHECKSIG_OUTPUT.replace("pkg.rpm", "a.rpm"), stderr="")
        with patch("pulp_tool.utils.rpm_gpg_verify.subprocess.run", return_value=ok):
            verify_pulled_rpms_gpg([rpm_path], gpg_keyring=None)

    def test_verify_pulled_rpms_missing_keyring_path(self, tmp_path: Path) -> None:
        rpm_path = tmp_path / "a.rpm"
        rpm_path.write_bytes(b"a")
        with pytest.raises(RpmGpgVerifyError, match="keyring path does not exist"):
            verify_pulled_rpms_gpg([rpm_path], gpg_keyring=str(tmp_path / "missing-gpg"))

    def test_verify_pulled_rpms_reuses_one_rpmdb(self, tmp_path: Path) -> None:
        keyring = tmp_path / "gpg-home"
        keyring.mkdir()
        first = tmp_path / "a.rpm"
        second = tmp_path / "b.rpm"
        first.write_bytes(b"a")
        second.write_bytes(b"b")
        rpmdb = tmp_path / "rpmdb"
        rpmdb.mkdir()
        with (
            patch(
                "pulp_tool.utils.rpm_gpg_verify._ephemeral_rpmdb_for_gnupg_home",
                return_value=_fake_cm(rpmdb),
            ) as mock_ephemeral,
            patch("pulp_tool.utils.rpm_gpg_verify._run_rpm_checksig") as mock_checksig,
        ):
            verify_pulled_rpms_gpg([first, second], gpg_keyring=str(keyring))
            mock_ephemeral.assert_called_once()
            assert mock_checksig.call_count == 2
            assert mock_checksig.call_args_list[0].kwargs["rpmdb_path"] == rpmdb
            assert mock_checksig.call_args_list[1].kwargs["rpmdb_path"] == rpmdb


def _fake_cm(value: Path):
    from contextlib import contextmanager

    @contextmanager
    def _cm():
        yield value

    return _cm()
