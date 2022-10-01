.PHONY: dkbuild dkrun

dkbuild:
	@docker build -t spark-samples .

dkrun:
	@docker run --entrypoint "" --rm -ti -v ${PWD}:/app -w /app spark-samples bash
