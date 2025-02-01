import pydicom
import numpy as np
from typing import Dict, Any, Tuple, List

class DicomProcessor:
    @staticmethod
    def clean_value(value: Any) -> Any:
        """Helper function to clean non-JSON-serializable values"""
        if isinstance(value, bytes):
            try:
                return value.decode('utf-8', 'ignore')
            except:
                return str(value)
        elif isinstance(value, (dict, list)):
            return DicomProcessor.clean_data(value)
        elif hasattr(value, 'original_string'):
            return str(value.original_string)
        elif hasattr(value, 'value'):
            return DicomProcessor.clean_value(value.value)
        elif hasattr(value, 'VR'):
            return str(value)
        elif isinstance(value, (int, float, str, bool, type(None))):
            return value
        elif hasattr(value, '__iter__') and not isinstance(value, (str, bytes, dict)):
            return [DicomProcessor.clean_value(item) for item in value]
        else:
            return str(value)

    @staticmethod
    def clean_data(data: Any) -> Any:
        """Recursively clean dictionary or list data"""
        if isinstance(data, dict):
            return {k: DicomProcessor.clean_value(v) for k, v in data.items()}
        elif isinstance(data, list):
            return [DicomProcessor.clean_value(item) for item in data]
        else:
            return DicomProcessor.clean_value(data)

    @staticmethod
    def process_dicom_metadata(dicom_dataset: pydicom.dataset.FileDataset) -> Dict[str, Any]:
        """Process DICOM metadata and format DirectoryRecordSequence appropriately"""
        metadata = {}
        record_types = {}
        
        for tag in dicom_dataset.dir():
            if tag != "PixelData":
                try:
                    value = dicom_dataset.get(tag)
                    if value is not None:
                        if tag == "DirectoryRecordSequence":
                            record_count = len(value)
                            for record in value:
                                if hasattr(record, "DirectoryRecordType"):
                                    record_type = record.DirectoryRecordType
                                    record_types[record_type] = record_types.get(record_type, 0) + 1
                            
                            metadata[tag] = {
                                "count": record_count,
                                "types": record_types
                            }
                        else:
                            if hasattr(value, 'is_sequence') and value.is_sequence:
                                sequence_data = []
                                for seq_item in value:
                                    item_dict = {}
                                    for elem in seq_item:
                                        if elem.keyword:
                                            item_dict[elem.keyword] = DicomProcessor.clean_value(elem.value)
                                    sequence_data.append(item_dict)
                                metadata[tag] = sequence_data
                            else:
                                metadata[tag] = DicomProcessor.clean_value(value)
                except Exception as e:
                    print(f"Error extracting metadata for tag {tag}: {e}")
                    metadata[tag] = f"Error extracting value: {str(e)}"
        
        return metadata 