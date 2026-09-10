.PHONY: all run forecast score prune tune dashboard conditions test

all: run

run:
	uv run barogram score
	uv run barogram forecast
	uv run barogram dashboard
	uv run barogram prune

forecast:
	uv run barogram forecast

score:
	uv run barogram score

prune:
	uv run barogram prune

tune:
	uv run barogram tune

dashboard:
	uv run barogram dashboard

conditions:
	uv run barogram conditions

test:
	uv run pytest tests/

