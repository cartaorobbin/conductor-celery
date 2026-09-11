# GitHub Review Posting

Load this reference when review mode is `github`.

## Goal

Post all inline comments atomically as a single **PENDING** review.
Do not submit it automatically.

## Build the review payload

The `owner` and `repo` come from `git remote get-url origin`.
Get the latest commit SHA from PR metadata (`headRefOid`).

### Via MCP

Create pending review:

```text
CallMcpTool: server=github, toolName=pull_request_review_write
arguments: { "owner": "<owner>", "repo": "<repo>", "pullNumber": <number>, "method": "create", "body": "<summary>", "commitID": "<headRefOid>" }
```

Add comments to the pending review:

```text
CallMcpTool: server=github, toolName=add_comment_to_pending_review
arguments: { "owner": "<owner>", "repo": "<repo>", "pullNumber": <number>, "path": "<path>", "line": <line>, "side": "RIGHT", "subjectType": "line", "body": "**[🟡 Important]** ..."}
```

Do not call submit.

### Via CLI

Create pending review by omitting `event`:

```bash
gh api repos/{owner}/{repo}/pulls/<number>/reviews \
  --method POST \
  -f body="<summary>" \
  -f commit_id="<headRefOid>" \
  --input <json-file>
```

`<json-file>` includes:

```json
{
  "body": "<summary>",
  "commit_id": "<sha>",
  "comments": [
    {
      "path": "app/file.py",
      "line": 42,
      "side": "RIGHT",
      "body": "**[🔴 Critical]** ..."
    }
  ]
}
```

## Diff line mapping

GitHub inline comments require diff positions, not raw file lines.

- Parse each file `patch`.
- Position is 1-based from the first `@@` line.
- Comment only on lines visible in the diff.
- If target line is not visible, create a file-level comment and prepend the line reference in text.

## Summary body format

```markdown
## Code Review Summary

**PR**: #<number> — <title>
**Branch**: <headRefName> → <baseRefName>
**Files reviewed**: <count>

### Findings
- 🔴 **Critical**: <count>
- 🟡 **Important**: <count>
- 🔵 **Suggestion**: <count>

### Convention Sources Loaded
- <source list, e.g. .cursor/rules, .claude/rules, knowledge, AGENTS/CLAUDE, selected skills>
- <missing sources, if any>

### Overview
<2-3 sentences>

### Key Observations
<main findings>

### What Looks Good
<well-written parts>
```

## Inline comment format

```markdown
**[🔴 Critical | 🟡 Important | 🔵 Suggestion]** — <description>

<suggested fix when useful>
```

## No findings

If there are no findings, still post a summary and explicitly state that no issues were found.
