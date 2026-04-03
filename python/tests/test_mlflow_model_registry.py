from __future__ import annotations

from types import SimpleNamespace

from mlflow_model_registry import (
    ensure_model_version_for_run,
    promote_model_alias_for_run,
    set_model_version_tags,
)


class FakeClient:
    def __init__(self) -> None:
        self.registered_models: dict[str, SimpleNamespace] = {}
        self.versions: list[SimpleNamespace] = []
        self.runs: dict[str, SimpleNamespace] = {}
        self.aliases: dict[tuple[str, str], str] = {}

    def get_registered_model(self, name: str):
        if name not in self.registered_models:
            raise KeyError(name)
        return self.registered_models[name]

    def create_registered_model(self, name: str):
        model = SimpleNamespace(name=name)
        self.registered_models[name] = model
        return model

    def search_model_versions(self, filter_string: str):
        model_name = filter_string.split("'")[1]
        return [version for version in self.versions if version.name == model_name]

    def get_run(self, run_id: str):
        return self.runs[run_id]

    def create_model_version(self, name: str, source: str, run_id: str):
        version = SimpleNamespace(
            name=name,
            version=str(len(self.versions) + 1),
            source=source,
            run_id=run_id,
            status='READY',
            tags={},
        )
        self.versions.append(version)
        return version

    def get_model_version(self, name: str, version: str):
        for candidate in self.versions:
            if candidate.name == name and str(candidate.version) == str(version):
                return candidate
        raise KeyError((name, version))

    def set_registered_model_alias(self, name: str, alias: str, version: str):
        self.aliases[(name, alias)] = str(version)

    def set_model_version_tag(self, name: str, version: str, key: str, value: str):
        model_version = self.get_model_version(name, version)
        model_version.tags[key] = value


def test_ensure_model_version_for_run_creates_registered_model_and_version():
    client = FakeClient()
    client.runs['run-1'] = SimpleNamespace(info=SimpleNamespace(artifact_uri='file:///tmp/mlruns/123/artifacts'))

    version = ensure_model_version_for_run(client, 'run-1')

    assert client.get_registered_model('ClimateTemperatureModel').name == 'ClimateTemperatureModel'
    assert version.version == '1'
    assert version.source.endswith('/model')
    assert version.run_id == 'run-1'


def test_ensure_model_version_for_run_reuses_existing_version():
    client = FakeClient()
    client.registered_models['ClimateTemperatureModel'] = SimpleNamespace(name='ClimateTemperatureModel')
    client.runs['run-1'] = SimpleNamespace(info=SimpleNamespace(artifact_uri='file:///tmp/mlruns/123/artifacts'))
    existing = client.create_model_version(
        name='ClimateTemperatureModel',
        source='file:///tmp/mlruns/123/artifacts/model',
        run_id='run-1',
    )

    version = ensure_model_version_for_run(client, 'run-1')

    assert version.version == existing.version
    assert len(client.versions) == 1


def test_promote_model_alias_for_run_sets_alias_and_tags():
    client = FakeClient()
    client.runs['run-2'] = SimpleNamespace(info=SimpleNamespace(artifact_uri='file:///tmp/mlruns/456/artifacts'))

    version = promote_model_alias_for_run(client, 'run-2')
    set_model_version_tags(
        client,
        'ClimateTemperatureModel',
        str(version.version),
        {'quality_gate': 'passed', 'promoted_alias': 'champion'},
    )

    assert client.aliases[('ClimateTemperatureModel', 'champion')] == str(version.version)
    assert client.get_model_version('ClimateTemperatureModel', str(version.version)).tags['quality_gate'] == 'passed'
