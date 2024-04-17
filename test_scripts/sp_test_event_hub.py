from azure.identity import DefaultAzureCredential, ClientSecretCredential
from azure.keyvault.secrets import SecretClient
import socket
from confluent_kafka import Producer, KafkaException
import time

def acked(err, msg):
    if err is not None:
        print(f"Failed to deliver message: {msg.value()}: {err.str()}")
    else:
        print(f"Message produced: {msg.value()}")

def get_credentials():
    # Authenticate to Key Vault
    credential = DefaultAzureCredential()
    key_vault_url = "https://nvs-key-vault.vault.azure.net/"
    client = SecretClient(vault_url=key_vault_url, credential=credential)

    # Retrieve secrets
    tenant_id = client.get_secret("TenantID").value
    client_id = client.get_secret("servicePrincipalApplicationId").value
    client_secret = client.get_secret("manual").value

    '''print(tenant_id)
    print(client_id)
    print(client_secret)
    servicePrincipalSecret'''

    return tenant_id, client_id, client_secret, credential

def create_producer(token):
    conf = {
        'bootstrap.servers': "nvs-event-hub-namespace.servicebus.windows.net:9093",
        'client.id': socket.gethostname(),
        'security.protocol': 'SASL_SSL',
        'sasl.mechanism': 'OAUTHBEARER',
        'sasl.oauthbearer.config': f'oauthbearer_token={token}',
        'ssl.ca.location': '/opt/homebrew/etc/openssl@3/cert.pem',
        'request.timeout.ms': 60000,
        'session.timeout.ms': 30000
    }
    return Producer(**conf)

def main():
    
    tenant_id, client_id, client_secret, azure_credential = get_credentials()

    # Initial token fetch
    event_hub_credential = ClientSecretCredential(tenant_id=tenant_id, client_id=client_id, client_secret=client_secret)
    token = event_hub_credential.get_token("https://eventhubs.azure.net/.default").token
    producer = create_producer(token)

    topic = 'stock-hub'
    try:
        for i in range(10000):
            message = f'Hello World {i}'
            producer.produce(topic, message.encode('utf-8'), callback=acked)
            producer.poll(1)
    except Exception as e:
        print(f'An error occurred: {e}')
    finally:
        # Wait for any outstanding messages to be delivered and report delivery result
        producer.flush(30)  # Wait up to 30 seconds.
    
if __name__ == '__main__':
    main()
