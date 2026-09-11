# Edge Cases and Error Recovery

Use this guide when execution is blocked or context is too large.

## Large PRs

If PR has more than 30 changed files, ask whether to review all files or focus specific areas.

```yaml
title: "Large PR"
questions:
  - id: scope
    prompt: "This PR has <N> changed files. Review all of them or focus on specific areas?"
    options:
      - id: all
        label: "Review all files"
      - id: focus
        label: "Let me specify which files or directories to focus on"
```

## Binary and generated files

Skip binary files, lock files, and generated artifacts. Mention skipped files in the summary.

## Permissions issues (403/404)

- Verify authentication (`gh auth status` or MCP `get_me`).
- If write permissions are missing, switch to local-only review mode.
- If PR number is invalid, confirm with the user.

## Very large diff (>2000 lines or >50 files)

- Offer scoped review by directory or pattern.
- Prioritize source files over config/generated files.
- Skip vendored dependencies.

## Existing pending review

- Check pending reviews:

```bash
gh api repos/{owner}/{repo}/pulls/<number>/reviews
```

- Ask user to delete stale review or append to it.

## Rate limit reached

- Post partial findings collected so far.
- List remaining unreviewed files.
- Offer to continue later or switch to local-only mode.

## Comment line not in diff

Convert to file-level comment and include the target line reference in the comment body.
