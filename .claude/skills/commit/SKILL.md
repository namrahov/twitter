---
name: commit
description: Write a commit message for the current changes, following this repo's "TWITTER-#: <summary>" convention, and print it for copy-pasting into IntelliJ's Commit Message field. Use when the user runs /commit or asks for a commit message. Never creates the commit itself.
allowed-tools: Bash(git status:*), Bash(git diff:*), Bash(git log:*), Bash(git branch:*), Bash(git rev-parse:*), Bash(git show:*)
---

# Commit message

Generate a commit message for the `twitter` project. **Output only — the user commits from IntelliJ.**

## Hard rules

- **Never** run `git add`, `git commit`, `git stash`, `git reset`, `git push`, or any command that changes the repository state. Read-only git commands only.
- **Never** write the message into a file (`COMMIT_EDITMSG`, hooks, etc.). It goes to the terminal so the user can copy it.
- The final answer ends with **one fenced code block containing nothing but the message** — no comments inside the block, no `git commit -m` wrapper, no quotes around it. It must be paste-ready for IntelliJ's *Commit Message* field.

`$ARGUMENTS` (optional, combinable):

| Argument | Meaning |
|---|---|
| _(empty)_ | Describe everything currently changed. |
| `az` | Write the message in Azerbaijani instead of English. |
| `TWITTER-12` | Use this ticket key instead of the one from the branch name. |
| `staged` | Describe only the staged changes. |
| any other text | Treat it as a hint about what the change is about. |

## 1. Collect context

Run these together (Bash tool, parallel calls are fine):

```bash
git branch --show-current
git status --porcelain=v1
git diff --stat && git diff                     # unstaged
git diff --cached --stat && git diff --cached   # staged
git log -n 15 --pretty=format:'%s'
```

Read the actual diff, not just the file names — the message must describe **behaviour**, not files. For new files that `git diff` does not show, read them directly.

Scope:
- `staged` passed, or something is already staged and the working tree is otherwise clean → describe the staged changes.
- Otherwise → describe every uncommitted change (staged + unstaged + new files).
- Nothing changed → say so and stop; do not invent a message.

## 2. Ticket key

1. From `$ARGUMENTS` if it contains a `TWITTER-\d+` token (case-insensitive).
2. Otherwise from the branch name, case-insensitive: `feature/twitter-1` → `TWITTER-1`. Always uppercase it.
3. If neither yields a key, produce the message without a prefix and mention that below the block.

## 3. Message format

```
TWITTER-1: Resolve current user from X-User-Id header

- Add @CurrentUser argument resolver for controller methods
- Hide the mechanism behind CurrentUserProvider so Spring Security can replace it later
- Return 401 when the header is missing, non-numeric or not positive
```

Rules:

- Subject: `<TICKET>: <imperative summary>`, ≤ 72 characters, no trailing dot. English by default; Azerbaijani only when `az` is passed.
- Body: only when the change has more than one meaningful point — otherwise subject alone. `- ` bullets, one fact per line, wrapped at ~100 chars. Say *what changed and why*, and name the touched endpoint / class / Liquibase changeset when it helps a reviewer.
- Never write filler like "update files", "fix bug", "changes", or a bare "refactor".
- Never collapse the whole message onto one line.
- No `Co-Authored-By` or other trailer: the user authors the commit.

## 4. Things worth flagging (outside the code block)

Keep these to one short line each, placed **before** the code block, and never let them block the output:

- Changes touching `src/main/resources/application*.yml`, `.env` or any config with a literal password/secret — the project rule is that DB passwords must not be in source files; remind the user to use `${DB_PASSWORD}` instead.
- An already-applied Liquibase changeset (`src/main/resources/db/changelog/changes/*.sql`) being edited instead of a new changeset added.
- Files that normally should not be committed: `build/`, `.gradle/`, `.idea/`, `*.iml`, `logs/`, `*.log`, `.env`.
- Two clearly unrelated changes in the working tree → suggest splitting them and offer a second message for the other part.
- Current branch is `master` → note that the commit probably belongs on a `feature/twitter-#` branch.

## 5. Output shape

Short note lines (only if there is something to flag), then:

````
```
TWITTER-1: <summary>

- <point>
- <point>
```
````

Nothing after the block except, at most, one sentence.
