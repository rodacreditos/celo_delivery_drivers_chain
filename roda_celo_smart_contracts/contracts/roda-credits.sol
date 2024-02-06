// SPDX-License-Identifier: MIT
pragma solidity ^0.8.9;

import "@openzeppelin/contracts/token/ERC721/ERC721.sol";
import "@openzeppelin/contracts/access/AccessControl.sol";
import "@openzeppelin/contracts/utils/math/SafeMath.sol"; // Import SafeMath library

/**
 * @title RodaCreditCOP
 * @dev This contract represents a credit token with outstanding balance, issuance date, and credit term.
 */
contract RodaCreditCOP is ERC721, AccessControl {
    using SafeMath for uint; // Use SafeMath for uint type

    bytes32 public constant MINTER_ROLE = keccak256("MINTER_ROLE");

    // Creating a mapping to store the principal for each token ID.
    mapping(uint => uint) public principal;
    // Creating a mapping to store the outstandingBalance for each token ID.
    mapping(uint => uint) public outstandingBalance;
    // Creating a mapping to store the issuanceDate for each token ID.
    mapping(uint => uint) public issuanceDate;
    // Creating a mapping to store the creditTerm for each token ID.
    mapping(uint => uint) public creditTerm;

    // Payment struct to store payment information
    struct Payment {
        uint id;
        uint amount;
        uint date;
    }

    // Mapping to store an array of payments for each token ID
    mapping(uint => mapping(uint => Payment)) public creditPayments;

    // Mapping to ensure the uniqueness of payment IDs
    mapping(uint => uint) public paymentToCredit;

    constructor() ERC721("RodaCreditCOP", "RCCOP") {
        _grantRole(DEFAULT_ADMIN_ROLE, msg.sender);
        _grantRole(MINTER_ROLE, msg.sender);
    }

    event CreditIssued(
        address indexed to,
        uint256 indexed tokenId,
        uint256 principal,
        uint totalRepaymentAmount,
        uint256 issuanceDate,
        uint256 creditTerm
    );

    event PaymentRecorded(
        uint256 indexed tokenId,
        uint256 paymentId,
        uint256 paymentAmount,
        uint256 paymentDate
    );

    function issueCredit(address to, uint creditId, uint _principal, uint totalRepaymentAmount, uint _issuanceDate, uint _creditTerm) public onlyRole(MINTER_ROLE) {
        principal[creditId] = _principal;
        outstandingBalance[creditId] = totalRepaymentAmount; // Setting the outstandingBalance at mint time.
        issuanceDate[creditId] = _issuanceDate; // Setting the issuanceDate from the parameter.
        creditTerm[creditId] = _creditTerm; // Setting the creditTerm from the parameter.

        _safeMint(to, creditId); // Effects should be called last.

        emit CreditIssued(to, creditId, _principal, totalRepaymentAmount, _issuanceDate, _creditTerm);
    }

    // Function to update the outstandingBalance and record the payment with paymentDate
    function recordPayment(uint creditId, uint paymentId, uint paymentAmount, uint paymentDate) public onlyRole(DEFAULT_ADMIN_ROLE) {
        require(paymentToCredit[paymentId] == 0); // (make sure IDs in the original database can't be zero)
        outstandingBalance[creditId] = outstandingBalance[creditId].sub(paymentAmount); // Safely subtract paymentAmount

        // Add the payment with the paymentDate to the payments mapping
        creditPayments[creditId][paymentId] = Payment({ id: paymentId, amount: paymentAmount, date: paymentDate });
        paymentToCredit[paymentId] = creditId;

        emit PaymentRecorded(creditId, paymentId, paymentAmount, paymentDate);
    }

    function supportsInterface(bytes4 interfaceId)
        public
        view
        override(ERC721, AccessControl)
        returns (bool)
    {
        return super.supportsInterface(interfaceId);
    }
}
