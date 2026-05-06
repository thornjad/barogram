.PHONY: all forecast score tune dashboard conditions test

all:
	uv run barogram run

forecast:
	uv run barogram forecast

score:
	uv run barogram score

tune:
	uv run barogram tune

dashboard:
	uv run barogram dashboard

conditions:
	uv run barogram conditions

test:
	uv run pytest tests/
