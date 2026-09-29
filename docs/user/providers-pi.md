# Pi

Install and authenticate [Pi](https://pi.dev) on the machine running your
environment, then enable it in **Settings > Providers**. See
[provider setup](./install.md#providers).

Pi is a multi-provider agent harness: one Pi installation serves models
from many upstream providers (Anthropic, OpenAI, Google, OpenRouter, and
more). Authenticate each upstream provider with Pi directly (`pi auth`,
API keys, or environment variables); T3 Code reuses Pi's credentials and
has no sign-in of its own.

## Provider and model defaults

**Default upstream provider** and **Default model** in the Pi provider
settings choose what new sessions start with. Leave both empty to use
Pi's own defaults. Switch models mid-thread from the model picker; the
switch applies to the running Pi session.

The model list comes from Pi's catalog and reflects the upstream
providers Pi knows about. Pi's `enabledModels` allowlist is treated as
favorites, not a filter: every catalog model stays selectable.

Models that support reasoning also offer a reasoning level in the model
picker, from off up to Pi's highest supported level. The levels offered
depend on the model, and the level applies to the running Pi session.

## Approvals

Pi follows the shared [permission modes](./permission-modes.md). Reads,
file search, and directory listings run without asking; commands, edits,
writes, and the bridged browser/device tools ask first in restricted
modes.

## Session storage

By default T3 Code keeps each Pi thread's conversation under its own state
directory, separate from everything else on the machine. Turn on **Use Pi
session directory** to store those conversations in Pi's own directory
(`~/.pi/agent`) instead, alongside the sessions your standalone `pi` runs
create. Use it when you want Pi's own tooling to see your T3 threads; leave
it off to keep T3 threads isolated.

## Refresh models and commands

After changing a Pi login or configuration, use **Refresh provider
status** in **Settings > Providers** for that environment. On mobile, use
**Refresh models** in the thread settings.

Existing threads keep their selected model even when it disappears from
the catalog. If Pi rejects that model, select an available one and retry.
