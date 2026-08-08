def scale_callback(direction: str) -> None:
    """
    Placeholder – replace with real provisioning logic.
    Example ideas:
      * `docker run -d … dmlf:latest agent`  (scale‑out)
      * `docker stop <agent-container>`      (scale‑in)
      * Kubernetes: `apps/v1.Scale` patch on the Agent Deployment
      * Cloud: call AWS ASG / GCP Instance Group API
    """
    print(f"[Autoscaler/default] >>> {direction.upper()} requested (no action taken)")