from pydantic import BaseModel, Field


class KafkaConfig(BaseModel):
    bootstrap_servers: str = Field(..., allow_mutation=False)
    group_id: str = Field(..., allowe_mutation=False)
    enable_auto_commit: bool = True

def test_create_configuration():
    config = KafkaConfig(bootstrap_servers='localhost:9092', group_id='test')
    assert config.bootstrap_servers == 'localhost:9092'
    assert config.group_id == 'test'
    assert config.enable_auto_commit

