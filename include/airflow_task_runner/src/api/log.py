from datetime import datetime
from enum import Enum
from pathlib import Path
from typing import Dict, Optional


class MessageType(Enum):
    OUTPUT = "log"
    EXEC = "execution_log"
    INFRA = "infra"
    RESULT = "results"
    ERROR = "error"


def log(
    message_type: MessageType, 
    message: str, 
    time: Optional[datetime] = None,
    log_file: str = None
) -> Dict[str, str]:
    """ 
    
    """
    if log_file:
        log_file=Path(log_file)
        log_file.touch(mode=33206)

        with open(log_file, 'a') as lf:
            lf.write(message+"\n")
       
    return {
        "type": message_type.value,
        "output": message,
        "date": time.isoformat() if time else datetime.now().isoformat(),
    }
