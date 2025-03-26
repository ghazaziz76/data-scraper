# src/processors/extractors/email_extractor.py

import re
from typing import List, Union, Dict, Optional

class EmailExtractor:
    """
    Extract email addresses from text content with validation features.
    """
    
    def __init__(self, strict_validation: bool = False):
        """
        Initialize the email extractor.
        
        Args:
            strict_validation: Whether to use strict validation rules
        """
        self.strict_validation = strict_validation
        
        # Standard email regex pattern
        self.standard_pattern = r'[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,}'
        
        # Stricter email pattern (RFC 5322 compliant)
        self.strict_pattern = r'(?:[a-z0-9!#$%&\'*+/=?^_`{|}~-]+(?:\.[a-z0-9!#$%&\'*+/=?^_`{|}~-]+)*|"(?:[\x01-\x08\x0b\x0c\x0e-\x1f\x21\x23-\x5b\x5d-\x7f]|\\[\x01-\x09\x0b\x0c\x0e-\x7f])*")@(?:(?:[a-z0-9](?:[a-z0-9-]*[a-z0-9])?\.)+[a-z0-9](?:[a-z0-9-]*[a-z0-9])?|\[(?:(?:25[0-5]|2[0-4][0-9]|[01]?[0-9][0-9]?)\.){3}(?:25[0-5]|2[0-4][0-9]|[01]?[0-9][0-9]?|[a-z0-9-]*[a-z0-9]:(?:[\x01-\x08\x0b\x0c\x0e-\x1f\x21-\x5a\x53-\x7f]|\\[\x01-\x09\x0b\x0c\x0e-\x7f])+)\])'
    
    def extract(self, text: str) -> List[str]:
        """
        Extract email addresses from text.
        
        Args:
            text: Text content to extract emails from
            
        Returns:
            List[str]: List of extracted email addresses
        """
        pattern = self.strict_pattern if self.strict_validation else self.standard_pattern
        emails = re.findall(pattern, text, re.IGNORECASE)
        
        # Remove duplicates while preserving order
        unique_emails = []
        seen = set()
        for email in emails:
            if email.lower() not in seen:
                unique_emails.append(email)
                seen.add(email.lower())
                
        return unique_emails
    
    def validate_email(self, email: str) -> bool:
        """
        Validate a single email address.
        
        Args:
            email: Email address to validate
            
        Returns:
            bool: True if valid, False otherwise
        """
        pattern = self.strict_pattern if self.strict_validation else self.standard_pattern
        return bool(re.fullmatch(pattern, email, re.IGNORECASE))
    
    def extract_with_domains(self, text: str) -> Dict[str, List[str]]:
        """
        Extract emails and group them by domain.
        
        Args:
            text: Text content to extract emails from
            
        Returns:
            Dict[str, List[str]]: Emails grouped by domain
        """
        emails = self.extract(text)
        result = {}
        
        for email in emails:
            # Extract the domain part
            try:
                domain = email.split('@')[1].lower()
                if domain not in result:
                    result[domain] = []
                result[domain].append(email)
            except IndexError:
                continue
                
        return result
    
    def filter_by_domains(self, emails: List[str], domains: List[str], exclude: bool = False) -> List[str]:
        """
        Filter emails by domains.
        
        Args:
            emails: List of email addresses
            domains: List of domains to filter by
            exclude: If True, exclude the specified domains instead of including them
            
        Returns:
            List[str]: Filtered list of email addresses
        """
        result = []
        domains = [d.lower() for d in domains]
        
        for email in emails:
            try:
                domain = email.split('@')[1].lower()
                domain_match = any(domain.endswith(d) for d in domains)
                
                if (domain_match and not exclude) or (not domain_match and exclude):
                    result.append(email)
            except IndexError:
                continue
                
        return result
    
    def extract_from_html(self, html_content: str) -> List[str]:
        """
        Extract email addresses from HTML content, handling HTML entities.
        
        Args:
            html_content: HTML content to extract emails from
            
        Returns:
            List[str]: List of extracted email addresses
        """
        # Simple HTML entity decoding for common email obfuscation techniques
        html_decoded = html_content
        html_decoded = re.sub(r'&#(\d+);', lambda m: chr(int(m.group(1))), html_decoded)
        html_decoded = re.sub(r'&(#x[0-9a-fA-F]+);', lambda m: chr(int(m.group(1)[2:], 16)), html_decoded)
        html_decoded = re.sub(r'<[^>]*>', ' ', html_decoded)  # Remove HTML tags
        
        return self.extract(html_decoded)


# src/processors/extractors/phone_extractor.py

import re
from typing import List, Dict, Optional, Union
import phonenumbers


class PhoneExtractor:
    """
    Extract phone numbers from text content with validation and formatting features.
    """
    
    def __init__(self, default_region: str = "US"):
        """
        Initialize the phone number extractor.
        
        Args:
            default_region: Default region code for parsing phone numbers (ISO 3166-1 alpha-2)
        """
        self.default_region = default_region
        
        # Common phone number patterns
        self.patterns = [
            r'\b(?:\+\d{1,3}[- ]?)?\(?\d{3}\)?[- ]?\d{3}[- ]?\d{4}\b',  # (123) 456-7890, +1 (123) 456-7890
            r'\b\d{3}[- ]?\d{3}[- ]?\d{4}\b',  # 123-456-7890, 123 456 7890
            r'\b(?:\+\d{1,3}[- ]?)?\d{5,}\b',  # +123456789012
        ]
    
    def extract(self, text: str) -> List[str]:
        """
        Extract phone numbers from text using regex patterns.
        
        Args:
            text: Text content to extract phone numbers from
            
        Returns:
            List[str]: List of extracted phone numbers
        """
        phone_numbers = []
        
        for pattern in self.patterns:
            matches = re.findall(pattern, text)
            phone_numbers.extend(matches)
        
        # Remove duplicates while preserving order
        unique_numbers = []
        seen = set()
        for phone in phone_numbers:
            # Normalize by removing non-digits
            normalized = ''.join(c for c in phone if c.isdigit())
            if normalized not in seen and len(normalized) >= 7:  # Minimum 7 digits
                unique_numbers.append(phone)
                seen.add(normalized)
                
        return unique_numbers
    
    def extract_with_library(self, text: str) -> List[Dict[str, str]]:
        """
        Extract phone numbers using the phonenumbers library for more accurate results.
        
        Args:
            text: Text content to extract phone numbers from
            
        Returns:
            List[Dict[str, str]]: List of extracted phone numbers with metadata
        """
        try:
            # Find all potential phone numbers
            matches = phonenumbers.PhoneNumberMatcher(text, self.default_region)
            results = []
            
            for match in matches:
                phone_number = match.number
                if phonenumbers.is_valid_number(phone_number):
                    results.append({
                        'number': phonenumbers.format_number(phone_number, phonenumbers.PhoneNumberFormat.INTERNATIONAL),
                        'national': phonenumbers.format_number(phone_number, phonenumbers.PhoneNumberFormat.NATIONAL),
                        'e164': phonenumbers.format_number(phone_number, phonenumbers.PhoneNumberFormat.E164),
                        'country_code': phone_number.country_code,
                        'region': phonenumbers.region_code_for_number(phone_number),
                        'type': self._get_number_type(phone_number),
                    })
            
            return results
        except ImportError:
            # Fallback to simple extraction if phonenumbers library is not available
            return [{'number': phone} for phone in self.extract(text)]
    
    def _get_number_type(self, phone_number) -> str:
        """Get the type of phone number"""
        try:
            number_type = phonenumbers.number_type(phone_number)
            type_map = {
                phonenumbers.PhoneNumberType.MOBILE: "mobile",
                phonenumbers.PhoneNumberType.FIXED_LINE: "landline",
                phonenumbers.PhoneNumberType.FIXED_LINE_OR_MOBILE: "landline_or_mobile",
                phonenumbers.PhoneNumberType.TOLL_FREE: "toll_free",
                phonenumbers.PhoneNumberType.PREMIUM_RATE: "premium_rate",
                phonenumbers.PhoneNumberType.SHARED_COST: "shared_cost",
                phonenumbers.PhoneNumberType.VOIP: "voip",
                phonenumbers.PhoneNumberType.PERSONAL_NUMBER: "personal",
                phonenumbers.PhoneNumberType.PAGER: "pager",
                phonenumbers.PhoneNumberType.UAN: "uan",
                phonenumbers.PhoneNumberType.VOICEMAIL: "voicemail",
                phonenumbers.PhoneNumberType.UNKNOWN: "unknown",
            }
            return type_map.get(number_type, "unknown")
        except:
            return "unknown"
    
    def validate_phone(self, phone: str) -> bool:
        """
        Validate a single phone number using the phonenumbers library.
        
        Args:
            phone: Phone number to validate
            
        Returns:
            bool: True if valid, False otherwise
        """
        try:
            parsed_number = phonenumbers.parse(phone, self.default_region)
            return phonenumbers.is_valid_number(parsed_number)
        except:
            return False
    
    def format_phone(self, phone: str, format_type: str = "international") -> str:
        """
        Format a phone number to a specific format.
        
        Args:
            phone: Phone number to format
            format_type: Format type ("international", "national", "e164")
            
        Returns:
            str: Formatted phone number or original if formatting fails
        """
        try:
            parsed_number = phonenumbers.parse(phone, self.default_region)
            
            if not phonenumbers.is_valid_number(parsed_number):
                return phone
                
            format_map = {
                "international": phonenumbers.PhoneNumberFormat.INTERNATIONAL,
                "national": phonenumbers.PhoneNumberFormat.NATIONAL,
                "e164": phonenumbers.PhoneNumberFormat.E164,
            }
            
            format_code = format_map.get(format_type.lower(), phonenumbers.PhoneNumberFormat.INTERNATIONAL)
            return phonenumbers.format_number(parsed_number, format_code)
        except:
            return phone
    
    def extract_from_html(self, html_content: str) -> List[str]:
        """
        Extract phone numbers from HTML content.
        
        Args:
            html_content: HTML content to extract phone numbers from
            
        Returns:
            List[str]: List of extracted phone numbers
        """
        # Remove HTML tags
        text = re.sub(r'<[^>]*>', ' ', html_content)
        return self.extract(text)


# src/processors/extractors/address_extractor.py

import re
from typing import List, Dict, Optional, Union
import usaddress


class AddressExtractor:
    """
    Extract physical addresses from text content with validation and parsing features.
    """
    
    def __init__(self, country: str = "US"):
        """
        Initialize the address extractor.
        
        Args:
            country: Country for address parsing (currently only US is fully supported)
        """
        self.country = country
        
        # Address patterns for different countries
        self.patterns = {
            "US": [
                # Street address with city, state, zip
                r'\d+\s+[A-Za-z0-9\s,\.]+(?:Avenue|Ave|Boulevard|Blvd|Circle|Cir|Court|Ct|Drive|Dr|Lane|Ln|Place|Pl|Plaza|Plz|Road|Rd|Square|Sq|Street|St|Way|Terrace|Ter|Trail|Trl)\.?(?:\s*,\s*|\s+)[A-Za-z\s]+(?:,\s*|\s+)[A-Z]{2}(?:\s*,\s*|\s+)\d{5}(?:-\d{4})?',
                
                # Just street address
                r'\d+\s+[A-Za-z0-9\s,\.]+(?:Avenue|Ave|Boulevard|Blvd|Circle|Cir|Court|Ct|Drive|Dr|Lane|Ln|Place|Pl|Plaza|Plz|Road|Rd|Square|Sq|Street|St|Way|Terrace|Ter|Trail|Trl)',
                
                # PO Box
                r'P\.?O\.?\s*Box\s+\d+(?:\s*,\s*|\s+)[A-Za-z\s]+(?:,\s*|\s+)[A-Z]{2}(?:\s*,\s*|\s+)\d{5}(?:-\d{4})?',
            ],
            "UK": [
                # UK address with postal code
                r'\d+\s+[A-Za-z0-9\s,\.]+(?:,\s*|\s+)[A-Za-z\s]+(?:,\s*|\s+)[A-Z]{1,2}\d{1,2}[A-Z]?\s+\d[A-Z]{2}',
            ],
            "CA": [
                # Canadian address with postal code
                r'\d+\s+[A-Za-z0-9\s,\.]+(?:,\s*|\s+)[A-Za-z\s]+(?:,\s*|\s+)[A-Z]{1}\d{1}[A-Z]{1}\s+\d{1}[A-Z]{1}\d{1}',
            ],
        }
    
    def extract(self, text: str) -> List[str]:
        """
        Extract addresses from text using regex patterns.
        
        Args:
            text: Text content to extract addresses from
            
        Returns:
            List[str]: List of extracted addresses
        """
        addresses = []
        
        # Get patterns for the specified country, or US as fallback
        country_patterns = self.patterns.get(self.country, self.patterns["US"])
        
        for pattern in country_patterns:
            matches = re.findall(pattern, text, re.IGNORECASE)
            addresses.extend(matches)
        
        # Remove duplicates while preserving order
        unique_addresses = []
        seen = set()
        for address in addresses:
            # Normalize whitespace
            normalized = ' '.join(address.split())
            if normalized not in seen:
                unique_addresses.append(normalized)
                seen.add(normalized)
                
        return unique_addresses
    
    def parse_address(self, address: str) -> Dict[str, str]:
        """
        Parse a US address into components using the usaddress library.
        
        Args:
            address: Address string to parse
            
        Returns:
            Dict[str, str]: Dictionary of address components
        """
        if self.country != "US":
            return {"full_address": address}
            
        try:
            # Parse address using usaddress library
            parsed, address_type = usaddress.tag(address)
            return parsed
        except (usaddress.RepeatedLabelError, UnicodeEncodeError):
            # Fallback for parsing errors
            return {"full_address": address}
        except ImportError:
            # Fallback if usaddress library is not available
            return {"full_address": address}
    
    def validate_us_address(self, address: str) -> bool:
        """
        Basic validation for US addresses.
        
        Args:
            address: Address string to validate
            
        Returns:
            bool: True if the address appears valid
        """
        if self.country != "US":
            return True
            
        try:
            # Check if we can parse it with usaddress
            parsed, address_type = usaddress.tag(address)
            
            # Basic validation: address should have at least a number and street name
            has_number = any(key.startswith('AddressNumber') for key in parsed)
            has_street = any(key.startswith('StreetName') for key in parsed)
            
            return has_number and has_street
        except:
            return False
    
    def extract_with_context(self, text: str, window_size: int = 100) -> List[Dict[str, str]]:
        """
        Extract addresses with surrounding context.
        
        Args:
            text: Text content to extract addresses from
            window_size: Number of characters to include as context before and after
            
        Returns:
            List[Dict[str, str]]: List of addresses with context
        """
        addresses = self.extract(text)
        results = []
        
        for address in addresses:
            # Find position in the original text
            start_pos = text.find(address)
            if start_pos >= 0:
                # Get context
                context_start = max(0, start_pos - window_size)
                context_end = min(len(text), start_pos + len(address) + window_size)
                context = text[context_start:context_end]
                
                results.append({
                    "address": address,
                    "context": context,
                    "parsed": self.parse_address(address)
                })
            else:
                results.append({
                    "address": address,
                    "context": "",
                    "parsed": self.parse_address(address)
                })
        
        return results
