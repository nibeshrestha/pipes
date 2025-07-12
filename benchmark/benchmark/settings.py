# Copyright(C) Facebook, Inc. and its affiliates.
from json import load, JSONDecodeError


class SettingsError(Exception):
    pass


class Settings:
    def __init__(
        self,
        deploy_key_name,
        deploy_key_path,
        instance_key_name, 
        instance_key_path,
        base_port, 
        repo_name, 
        repo_url,
        branch, 
        instance_type, 
        zones
    ):
        inputs_str = [
            instance_key_name, instance_key_path, repo_name, repo_url, branch, instance_type
        ]
        if isinstance(zones, list):
            zones = zones
        else:
            zones = [zones]
        inputs_str += zones
        ok = all(isinstance(x, str) for x in inputs_str)
        ok &= isinstance(base_port, int)
        ok &= len(zones) > 0
        if not ok:
            raise SettingsError('Invalid settings types')

        self.github_deploy_key_name = deploy_key_name
        self.github_deploy_key_path = deploy_key_path
        self.key_name = instance_key_name
        self.key_path = instance_key_path

        self.base_port = base_port

        self.repo_name = repo_name
        self.repo_url = repo_url
        self.branch = branch

        self.instance_type = instance_type
        self.zones = zones

    @classmethod
    def load(cls, filename):
        try:
            with open(filename, 'r') as f:
                data = load(f)

            return cls(
                data['github_deploy_key']['name'],
                data['github_deploy_key']['path'],
                data['key']['name'],
                data['key']['path'],
                data['port'],
                data['repo']['name'],
                data['repo']['url'],
                data['repo']['branch'],
                data['instances']['machine_type'],
                data['instances']['zones'],
            )
        except (OSError, JSONDecodeError) as e:
            raise SettingsError(str(e))

        except KeyError as e:
            raise SettingsError(f'Malformed settings: missing key {e}')