.PHONY: all run full forecast score prune tune tune-weights tune-similarity dashboard conditions test

all: run

run:
	uv run barogram score
	uv run barogram forecast
	uv run barogram dashboard
	uv run barogram prune

full: tune run

forecast:
	uv run barogram forecast

score:
	uv run barogram score

prune:
	uv run barogram prune

tune: tune-weights
	uv run barogram calibration

tune-weights: score
	uv run barogram tune

tune-similarity:
	uv run barogram tune-similarity

dashboard:
	uv run barogram dashboard

conditions:
	uv run barogram conditions

test:
	uv run pytest tests/

