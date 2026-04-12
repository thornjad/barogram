.PHONY: all forecast score dashboard conditions

all:
	uv run barogram run

forecast:
	uv run barogram forecast

score:
	uv run barogram score

dashboard:
	uv run barogram dashboard

conditions:
	uv run barogram conditions
