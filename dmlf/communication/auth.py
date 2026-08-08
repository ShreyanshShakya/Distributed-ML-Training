import grpc
from typing import Set


class TokenClientInterceptor(grpc.UnaryUnaryClientInterceptor,
                             grpc.StreamStreamClientInterceptor,
                             grpc.UnaryStreamClientInterceptor,
                             grpc.StreamUnaryClientInterceptor):
    """Adds Authorization metadata to every outgoing RPC."""
    def __init__(self, token: str):
        self._token = token

    def _add_auth(self, metadata):
        if metadata is None:
            metadata = []
        return list(metadata) + [("authorization", f"Bearer {self._token}")]

    def intercept_unary_unary(self, continuation, client_call_details, request):
        new_details = client_call_details._replace(
            metadata=self._add_auth(client_call_details.metadata)
        )
        return continuation(new_details, request)

    def intercept_stream_stream(self, continuation, client_call_details, request_iterator):
        new_details = client_call_details._replace(
            metadata=self._add_auth(client_call_details.metadata)
        )
        return continuation(new_details, request_iterator)

    def intercept_unary_stream(self, continuation, client_call_details, request):
        new_details = client_call_details._replace(
            metadata=self._add_auth(client_call_details.metadata)
        )
        return continuation(new_details, request)

    def intercept_stream_unary(self, continuation, client_call_details, request_iterator):
        new_details = client_call_details._replace(
            metadata=self._add_auth(client_call_details.metadata)
        )
        return continuation(new_details, request_iterator)


class TokenServerInterceptor(grpc.ServerInterceptor):
    """Validates Authorization metadata on incoming RPCs."""
    def __init__(self, token: str):
        self._token = token

    def intercept_service(self, continuation, handler_call_details):
        meta = dict(handler_call_details.invocation_metadata or [])
        token = meta.get("authorization", "").removeprefix("Bearer ").strip()
        if token != self._token:
            def abort(_, context):
                context.abort(grpc.StatusCode.UNAUTHENTICATED, "invalid token")
            return grpc.unary_unary_rpc_method_handler(abort)
        return continuation(handler_call_details)


class NodeSecretClientInterceptor(grpc.UnaryUnaryClientInterceptor,
                                  grpc.StreamStreamClientInterceptor,
                                  grpc.UnaryStreamClientInterceptor,
                                  grpc.StreamUnaryClientInterceptor):
    """Adds node-secret metadata when node_id is known."""
    def __init__(self):
        self.node_secret = None
        self.node_id = None

    def set_credentials(self, node_id: str, node_secret: str):
        self.node_id = node_id
        self.node_secret = node_secret

    def _add_node_secret(self, metadata):
        if metadata is None:
            metadata = []
        md = list(metadata)
        if self.node_id:
            md.append(("node-id", self.node_id))
        if self.node_secret:
            md.append(("node-secret", self.node_secret))
        return md

    def intercept_unary_unary(self, continuation, client_call_details, request):
        new_details = client_call_details._replace(
            metadata=self._add_node_secret(client_call_details.metadata)
        )
        return continuation(new_details, request)

    def intercept_stream_stream(self, continuation, client_call_details, request_iterator):
        new_details = client_call_details._replace(
            metadata=self._add_node_secret(client_call_details.metadata)
        )
        return continuation(new_details, request_iterator)

    def intercept_unary_stream(self, continuation, client_call_details, request):
        new_details = client_call_details._replace(
            metadata=self._add_node_secret(client_call_details.metadata)
        )
        return continuation(new_details, request)

    def intercept_stream_unary(self, continuation, client_call_details, request_iterator):
        new_details = client_call_details._replace(
            metadata=self._add_node_secret(client_call_details.metadata)
        )
        return continuation(new_details, request_iterator)


# RPC methods that require node authentication (token + node-secret)
NODE_AUTH_RPCS: Set[str] = {
    "/cml.ClusterManager/SendHeartbeat",
    "/cml.ClusterManager/ListenForCommands",
    "/cml.ClusterManager/ReportJobStatus",
    "/cml.ClusterManager/StreamLogs",
}


class NodeSecretServerInterceptor(grpc.ServerInterceptor):
    """Validates node-secret metadata for RPCs that require node authentication."""
    def __init__(self, registry):
        self.registry = registry

    def intercept_service(self, continuation, handler_call_details):
        method = handler_call_details.method
        meta = dict(handler_call_details.invocation_metadata or [])
        node_id = meta.get("node-id")
        node_secret = meta.get("node-secret")

        # For node-authenticated RPCs, require both node-id and node-secret
        if method in NODE_AUTH_RPCS:
            if not node_id or not node_secret:
                def abort(_, context):
                    context.abort(grpc.StatusCode.UNAUTHENTICATED, "missing node credentials")
                return grpc.unary_unary_rpc_method_handler(abort)
            
            # Verify secret
            stored = self.registry.get_node_secret(node_id)
            if stored != node_secret:
                def abort(_, context):
                    context.abort(grpc.StatusCode.UNAUTHENTICATED, "invalid node secret")
                return grpc.unary_unary_rpc_method_handler(abort)

        return continuation(handler_call_details)


def create_client_interceptors(token: str) -> list:
    """Create client interceptors with the given token."""
    return [
        TokenClientInterceptor(token),
        NodeSecretClientInterceptor(),
    ]


def create_server_interceptors(token: str, registry) -> list:
    """Create server interceptors with the given token and registry."""
    return [
        TokenServerInterceptor(token),
        NodeSecretServerInterceptor(registry),
    ]