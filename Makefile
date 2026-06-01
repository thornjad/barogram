.PHONY: all run forecast score tune dashboard conditions test publish

all: run

run:
	uv run barogram score
	uv run barogram forecast
	uv run barogram dashboard

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

publish:
	/Users/jmt/.local/bin/barogram-publish
