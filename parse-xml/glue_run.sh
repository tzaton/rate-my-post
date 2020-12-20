#!/bin/bash

aws glue start-workflow-run \
--name "$TASK_PARSE_XML"
