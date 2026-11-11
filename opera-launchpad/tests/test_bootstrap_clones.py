"""Tool clones off the shared repo cache: any branch, the remote's current tip, and a pin that changes.

Uses real git repos under tmp_path: a bare "remote" with a default and a feature branch, the launchpad's cache of
it, and tool clones made from that cache.
"""
import subprocess
import sys
from pathlib import Path

import pytest

HERE = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(HERE))

import bootstrap  # noqa: E402


def _git(cwd, *args):
    return subprocess.run(["git", *args], cwd=cwd, check=True, capture_output=True, text=True).stdout.strip()


def _commit(work, message):
    (work / "notes.txt").write_text(message + "\n")
    _git(work, "add", "notes.txt")
    _git(work, "commit", "-q", "-m", message)
    return _git(work, "rev-parse", "HEAD")


@pytest.fixture
def launchpad(tmp_path, monkeypatch):
    """A bare remote (default branch develop, plus feature), a work tree to push from, and the launchpad dirs."""
    for var, value in {"GIT_AUTHOR_NAME": "t", "GIT_AUTHOR_EMAIL": "t@example.com",
                       "GIT_COMMITTER_NAME": "t", "GIT_COMMITTER_EMAIL": "t@example.com",
                       "GIT_CONFIG_GLOBAL": str(tmp_path / "gitconfig"), "GIT_CONFIG_NOSYSTEM": "1"}.items():
        monkeypatch.setenv(var, value)
    remote = tmp_path / "remote" / "tool-repo.git"
    remote.parent.mkdir()
    _git(tmp_path, "init", "-q", "--bare", "-b", "develop", str(remote))
    work = tmp_path / "work"
    _git(tmp_path, "clone", "-q", str(remote), str(work))
    _git(work, "checkout", "-q", "-b", "develop")
    _commit(work, "first")
    _git(work, "push", "-q", "origin", "develop")
    _git(work, "checkout", "-q", "-b", "feature")
    _commit(work, "feature work")
    _git(work, "push", "-q", "origin", "feature")
    _git(work, "checkout", "-q", "develop")

    root = tmp_path / "launchpad"
    monkeypatch.setattr(bootstrap, "ROOT", root)
    monkeypatch.setattr(bootstrap, "REPOS_DIR", root / "repos")
    monkeypatch.setattr(bootstrap, "REPO_CACHE_DIR", root / "repos" / "_cache")
    bootstrap.ensure_repo_cache(str(remote), update=False)
    return {"url": str(remote), "work": work, "root": root}


def _tool_dir(lp, name="audit"):
    return lp["root"] / "repos" / "PRODUCT" / name


def test_tool_pinned_to_a_non_default_branch_is_cloned(launchpad):
    dest = _tool_dir(launchpad)
    bootstrap.clone_or_update_repo(launchpad["url"], "feature", dest, update=False)

    assert _git(dest, "rev-parse", "--abbrev-ref", "HEAD") == "feature"
    assert _git(dest, "rev-parse", "HEAD") == _git(launchpad["work"], "rev-parse", "feature")
    assert _git(dest, "rev-parse", "--abbrev-ref", "feature@{upstream}") == "origin/feature"
    assert _git(dest, "remote", "get-url", "origin") == launchpad["url"]
    # Only the pinned branch is left: nothing from the cache's default branch.
    assert _git(dest, "for-each-ref", "--format=%(refname)", "refs/heads", "refs/remotes") == (
        "refs/heads/feature\nrefs/remotes/origin/feature")
    assert (dest / "notes.txt").read_text() == "feature work\n"
    assert bootstrap.check_repo_sync(dest, "feature")["up_to_date"] is True


def test_new_clone_is_at_the_remote_tip_not_the_cache_copy(launchpad):
    tip = _commit(launchpad["work"], "after the cache was made")
    _git(launchpad["work"], "push", "-q", "origin", "develop")

    dest = _tool_dir(launchpad)
    bootstrap.clone_or_update_repo(launchpad["url"], "develop", dest, update=False)

    assert _git(dest, "rev-parse", "HEAD") == tip
    assert bootstrap.check_repo_sync(dest, "develop") == {
        "cloned": True, "up_to_date": True, "behind": 0, "ahead": 0, "error": None}


def test_update_moves_a_clone_to_a_new_pin(launchpad):
    dest = _tool_dir(launchpad)
    bootstrap.clone_or_update_repo(launchpad["url"], "develop", dest, update=False)

    # Before the update, the sync check already compares against the new pin instead of erroring.
    status = bootstrap.check_repo_sync(dest, "feature")
    assert status["error"] is None and status["behind"] == 1

    bootstrap.clone_or_update_repo(launchpad["url"], "feature", dest, update=True)

    assert _git(dest, "rev-parse", "--abbrev-ref", "HEAD") == "feature"
    assert _git(dest, "rev-parse", "HEAD") == _git(launchpad["work"], "rev-parse", "feature")
    assert bootstrap.check_repo_sync(dest, "feature")["up_to_date"] is True


def test_update_keeps_following_the_pinned_branch(launchpad):
    dest = _tool_dir(launchpad)
    bootstrap.clone_or_update_repo(launchpad["url"], "feature", dest, update=False)
    _git(launchpad["work"], "checkout", "-q", "feature")
    tip = _commit(launchpad["work"], "more feature work")
    _git(launchpad["work"], "push", "-q", "origin", "feature")

    assert bootstrap.check_repo_sync(dest, "feature")["behind"] == 1
    bootstrap.clone_or_update_repo(launchpad["url"], "feature", dest, update=True)
    assert _git(dest, "rev-parse", "HEAD") == tip


def test_missing_branch_fails_and_leaves_no_half_made_clone(launchpad):
    dest = _tool_dir(launchpad)
    with pytest.raises(RuntimeError, match="no-such-branch"):
        bootstrap.clone_or_update_repo(launchpad["url"], "no-such-branch", dest, update=False)
    # A leftover directory would read as "Already cloned" on the next run.
    assert not dest.exists()
