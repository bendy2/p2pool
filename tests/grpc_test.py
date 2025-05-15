import grpc
import wallet_pb2
import wallet_pb2_grpc
from google.protobuf.empty_pb2 import Empty
import base64

def transfer_funds(
    grpc_host: str,
    grpc_port: int,
    username: str,
    password: str,
    recipient_address: str,
    amount: int,
    fee_per_gram: int,
    message: str
):
    """
    Transfer XTM to a recipient address via Tari wallet gRPC interface with Basic authentication.
    
    Args:
        grpc_host (str): Wallet gRPC server host (e.g., 'localhost')
        grpc_port (int): Wallet gRPC server port (e.g., 18143)
        username (str): Username for Basic authentication
        password (str): Password for Basic authentication
        recipient_address (str): Recipient's Tari address (hex or base58)
        amount (int): Amount to transfer in microTari (1 XTM = 1,000,000 microTari)
        fee_per_gram (int): Fee per gram for the transaction
        message (str): Transaction message
    """
    try:
        # Create Basic authentication header
        credentials = f"{username}:{password}"
        encoded_credentials = base64.b64encode(credentials.encode()).decode()
        auth_header = f"Basic {encoded_credentials}"

        # Establish gRPC channel (insecure for local testing; use TLS in production)
        channel = grpc.insecure_channel(f"{grpc_host}:{grpc_port}")
        
        # Add authentication metadata
        metadata = [('authorization', auth_header)]
        wallet_stub = wallet_pb2_grpc.WalletStub(channel)

        # Create transfer request
        transfer_request = wallet_pb2.TransferRequest(
            recipients=[
                wallet_pb2.TransferRecipient(
                    address=recipient_address,  # Recipient's address
                    amount=amount,             # Amount in microTari
                    fee_per_gram=fee_per_gram, # Transaction fee per gram
                    message=message            # Transaction message
                )
            ]
        )

        # Send transfer request with metadata
        response = wallet_stub.Transfer(transfer_request, metadata=metadata)

        # Check response
        if response.success:
            print(f"Transfer successful! Transaction ID: {response.transaction_id}")
            print(f"Details: {response.message}")
        else:
            print(f"Transfer failed: {response.message}")

        # Close channel
        channel.close()

    except grpc.RpcError as e:
        print(f"gRPC error occurred: {e}")
    except Exception as e:
        print(f"Unexpected error: {e}")

def main():
    # Configuration
    GRPC_HOST = "localhost"                     # Wallet gRPC host
    GRPC_PORT = 18143                          # Wallet gRPC port
    USERNAME = "bendy"            # Replace with actual username
    PASSWORD = "gucloud"            # Replace with actual password
    RECIPIENT_ADDRESS = "12GiRMnB7vcFMvmoW1wdm7wyfvRnAuBRnjP4GaLuWrhb5NKuyxda3xQckhVJ4S4mPBvhoSfixTDk3BFMvVjmr166539"  # Replace with actual Tari address
    AMOUNT = 1000000                           # 1 XTM = 1,000,000 microTari
    FEE_PER_GRAM = 25                          # Default fee per gram
    MESSAGE = "Auto transfer via gRPC"         # Transaction message

    # Execute transfer
    transfer_funds(
        grpc_host=GRPC_HOST,
        grpc_port=GRPC_PORT,
        username=USERNAME,
        password=PASSWORD,
        recipient_address=RECIPIENT_ADDRESS,
        amount=AMOUNT,
        fee_per_gram=FEE_PER_GRAM,
        message=MESSAGE
    )

if __name__ == "__main__":
    main()