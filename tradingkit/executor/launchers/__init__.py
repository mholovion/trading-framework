from tradingkit.executor.launchers.base import RunnerLauncher
from tradingkit.executor.launchers.docker_ import DockerRunnerLauncher
from tradingkit.executor.launchers.subprocess_ import SubprocessRunnerLauncher


def auto_launcher() -> RunnerLauncher:
    """
    Pick the best available launcher automatically:
      - DockerRunnerLauncher  if docker-py is installed and Docker daemon is reachable
      - SubprocessRunnerLauncher  otherwise (dev / no Docker)
    """
    try:
        import docker
        docker.from_env().ping()
        return DockerRunnerLauncher()
    except Exception:
        return SubprocessRunnerLauncher()


__all__ = [
    "DockerRunnerLauncher",
    "RunnerLauncher",
    "SubprocessRunnerLauncher",
    "auto_launcher",
]
