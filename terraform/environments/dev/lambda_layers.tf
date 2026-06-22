module "common_python_layer" {
  source = "../../modules/lambda_layer"


  layer_name  = "${local.name_prefix}-common-python"
  description = "Common Python dependencies and shared helpers for Lambda functions."


  source_dir = abspath("${path.root}/../../../.build/layers/common-python")

  compatible_runtimes      = ["python3.12"]
  compatible_architectures = ["arm64"]
}



module "websocket_python_layer" {
  source = "../../modules/lambda_layer"

  layer_name  = "${local.name_prefix}-websocket-python"
  description = "Shared Python helpers for WebSocket-related Lambda functions."

  source_dir = abspath("${path.root}/../../../.build/layers/websocket-python")

  compatible_runtimes      = ["python3.12"]
  compatible_architectures = ["arm64"]
}