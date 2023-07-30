# fsdedupe

[![Go Reference](https://pkg.go.dev/badge/github.com/mxmCherry/fsdedupe.svg)](https://pkg.go.dev/github.com/mxmCherry/fsdedupe)
[![Test](https://github.com/mxmCherry/fsdedupe/actions/workflows/test.yml/badge.svg?branch=main)](https://github.com/mxmCherry/fsdedupe/actions/workflows/test.yml)

Utils to deduplicate local filesystem

```shell
go install github.com/mxmCherry/fsdedupe/cmd/fsdedupe@latest

find <SOMEDIR> -type f -not -path '*/.*' | fsdedupe symlink
```
