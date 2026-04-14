#!/bin/zsh
# run barogram tune on the first wednesday of the month only
[[ $(date +%u) -eq 3 && $(date +%d) -le 7 ]] || exit 0
cd /Users/jmt/Documents/thornlog/barogram
/Users/jmt/.local/bin/uv run barogram tune
