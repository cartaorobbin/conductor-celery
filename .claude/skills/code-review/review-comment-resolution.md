# Review Comment Resolution

Load this reference when the user asks to fix open review comments.

## When to run

Examples:
- "leia os comentarios e resolva"
- "resolve os comentarios do PR"
- "fix the review comments"

## Step 1: Fetch open comments

```bash
gh api repos/{owner}/{repo}/pulls/<number>/comments \
  --jq '.[] | {id: .id, path: .path, body: .body, position: .position, line: .line}'
```

Read all comments before editing.

## Step 2: Apply fixes

For each actionable comment:
- Update code.
- Group related changes logically.

## Step 3: Run tests before push

Never push or resolve before tests pass.

```bash
make test
```

If tests fail, fix and rerun.

## Step 4: Commit and push

Use conventional commits and push:

```bash
git push
```

## Step 5: Ask user confirmation

Stop and ask before replying/resolving on GitHub:

> Todos os comentários foram resolvidos e os testes passando. Posso submeter as respostas e marcar os threads como resolvidos no GitHub?

Proceed only after explicit confirmation.

## Step 6: Reply and resolve threads

### 6a Reply

```bash
gh api --method POST \
  repos/{owner}/{repo}/pulls/<number>/comments/<comment_id>/replies \
  --field body="Resolvido: <descrição curta>"
```

### 6b Resolve thread

Get thread IDs:

```bash
gh api graphql -f query='
  query($owner:String!, $repo:String!, $pr:Int!) {
    repository(owner:$owner, name:$repo) {
      pullRequest(number:$pr) {
        reviewThreads(first:50) {
          nodes {
            id
            isResolved
            comments(first:1) { nodes { databaseId body } }
          }
        }
      }
    }
  }
' -f owner={owner} -f repo={repo} -F pr=<number>
```

Resolve by thread ID:

```bash
gh api graphql -f query='
  mutation($threadId:ID!) {
    resolveReviewThread(input:{threadId:$threadId}) {
      thread { id isResolved }
    }
  }
' -f threadId=<thread_id>
```
