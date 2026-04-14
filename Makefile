.PHONY: all forecast score tune dashboard conditions

all:
	uv run barogram run

forecast:
	uv run barogram forecast

score:
	uv run barogram score

tune:
	uv run barogram tune
	uv run barogram dashboard

dashboard:
	uv run barogram dashboard

conditions:
	uv run barogram conditions
