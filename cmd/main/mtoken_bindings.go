// Code generated - DO NOT EDIT.
// This file is a generated binding and any manual changes will be lost.

package main

import (
	"errors"
	"math/big"
	"strings"

	ethereum "github.com/ethereum/go-ethereum"
	"github.com/ethereum/go-ethereum/accounts/abi"
	"github.com/ethereum/go-ethereum/accounts/abi/bind"
	"github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/core/types"
	"github.com/ethereum/go-ethereum/event"
)

// Reference imports to suppress errors if they are not otherwise used.
var (
	_ = errors.New
	_ = big.NewInt
	_ = strings.NewReader
	_ = ethereum.NotFound
	_ = bind.Bind
	_ = common.Big1
	_ = types.BloomLookup
	_ = event.NewSubscription
	_ = abi.ConvertType
)

// MTokenMetaData contains all meta data concerning the MToken contract.
var MTokenMetaData = &bind.MetaData{
	ABI: "[{\"constant\":true,\"inputs\":[{\"name\":\"account\",\"type\":\"address\"}],\"name\":\"getAccountSnapshot\",\"outputs\":[{\"name\":\"\",\"type\":\"uint256\"},{\"name\":\"\",\"type\":\"uint256\"},{\"name\":\"\",\"type\":\"uint256\"},{\"name\":\"\",\"type\":\"uint256\"}],\"payable\":false,\"stateMutability\":\"view\",\"type\":\"function\"},{\"constant\":true,\"inputs\":[],\"name\":\"exchangeRateStored\",\"outputs\":[{\"name\":\"\",\"type\":\"uint256\"}],\"payable\":false,\"stateMutability\":\"view\",\"type\":\"function\"},{\"constant\":true,\"inputs\":[{\"name\":\"account\",\"type\":\"address\"}],\"name\":\"balanceOf\",\"outputs\":[{\"name\":\"\",\"type\":\"uint256\"}],\"payable\":false,\"stateMutability\":\"view\",\"type\":\"function\"},{\"constant\":true,\"inputs\":[],\"name\":\"underlying\",\"outputs\":[{\"name\":\"\",\"type\":\"address\"}],\"payable\":false,\"stateMutability\":\"view\",\"type\":\"function\"}]",
}

// MTokenABI is the input ABI used to generate the binding from.
// Deprecated: Use MTokenMetaData.ABI instead.
var MTokenABI = MTokenMetaData.ABI

// MToken is an auto generated Go binding around an Ethereum contract.
type MToken struct {
	MTokenCaller     // Read-only binding to the contract
	MTokenTransactor // Write-only binding to the contract
	MTokenFilterer   // Log filterer for contract events
}

// MTokenCaller is an auto generated read-only Go binding around an Ethereum contract.
type MTokenCaller struct {
	contract *bind.BoundContract // Generic contract wrapper for the low level calls
}

// MTokenTransactor is an auto generated write-only Go binding around an Ethereum contract.
type MTokenTransactor struct {
	contract *bind.BoundContract // Generic contract wrapper for the low level calls
}

// MTokenFilterer is an auto generated log filtering Go binding around an Ethereum contract events.
type MTokenFilterer struct {
	contract *bind.BoundContract // Generic contract wrapper for the low level calls
}

// MTokenSession is an auto generated Go binding around an Ethereum contract,
// with pre-set call and transact options.
type MTokenSession struct {
	Contract     *MToken           // Generic contract binding to set the session for
	CallOpts     bind.CallOpts     // Call options to use throughout this session
	TransactOpts bind.TransactOpts // Transaction auth options to use throughout this session
}

// MTokenCallerSession is an auto generated read-only Go binding around an Ethereum contract,
// with pre-set call options.
type MTokenCallerSession struct {
	Contract *MTokenCaller // Generic contract caller binding to set the session for
	CallOpts bind.CallOpts // Call options to use throughout this session
}

// MTokenTransactorSession is an auto generated write-only Go binding around an Ethereum contract,
// with pre-set transact options.
type MTokenTransactorSession struct {
	Contract     *MTokenTransactor // Generic contract transactor binding to set the session for
	TransactOpts bind.TransactOpts // Transaction auth options to use throughout this session
}

// MTokenRaw is an auto generated low-level Go binding around an Ethereum contract.
type MTokenRaw struct {
	Contract *MToken // Generic contract binding to access the raw methods on
}

// MTokenCallerRaw is an auto generated low-level read-only Go binding around an Ethereum contract.
type MTokenCallerRaw struct {
	Contract *MTokenCaller // Generic read-only contract binding to access the raw methods on
}

// MTokenTransactorRaw is an auto generated low-level write-only Go binding around an Ethereum contract.
type MTokenTransactorRaw struct {
	Contract *MTokenTransactor // Generic write-only contract binding to access the raw methods on
}

// NewMToken creates a new instance of MToken, bound to a specific deployed contract.
func NewMToken(address common.Address, backend bind.ContractBackend) (*MToken, error) {
	contract, err := bindMToken(address, backend, backend, backend)
	if err != nil {
		return nil, err
	}
	return &MToken{MTokenCaller: MTokenCaller{contract: contract}, MTokenTransactor: MTokenTransactor{contract: contract}, MTokenFilterer: MTokenFilterer{contract: contract}}, nil
}

// NewMTokenCaller creates a new read-only instance of MToken, bound to a specific deployed contract.
func NewMTokenCaller(address common.Address, caller bind.ContractCaller) (*MTokenCaller, error) {
	contract, err := bindMToken(address, caller, nil, nil)
	if err != nil {
		return nil, err
	}
	return &MTokenCaller{contract: contract}, nil
}

// NewMTokenTransactor creates a new write-only instance of MToken, bound to a specific deployed contract.
func NewMTokenTransactor(address common.Address, transactor bind.ContractTransactor) (*MTokenTransactor, error) {
	contract, err := bindMToken(address, nil, transactor, nil)
	if err != nil {
		return nil, err
	}
	return &MTokenTransactor{contract: contract}, nil
}

// NewMTokenFilterer creates a new log filterer instance of MToken, bound to a specific deployed contract.
func NewMTokenFilterer(address common.Address, filterer bind.ContractFilterer) (*MTokenFilterer, error) {
	contract, err := bindMToken(address, nil, nil, filterer)
	if err != nil {
		return nil, err
	}
	return &MTokenFilterer{contract: contract}, nil
}

// bindMToken binds a generic wrapper to an already deployed contract.
func bindMToken(address common.Address, caller bind.ContractCaller, transactor bind.ContractTransactor, filterer bind.ContractFilterer) (*bind.BoundContract, error) {
	parsed, err := MTokenMetaData.GetAbi()
	if err != nil {
		return nil, err
	}
	return bind.NewBoundContract(address, *parsed, caller, transactor, filterer), nil
}

// Call invokes the (constant) contract method with params as input values and
// sets the output to result. The result type might be a single field for simple
// returns, a slice of interfaces for anonymous returns and a struct for named
// returns.
func (_MToken *MTokenRaw) Call(opts *bind.CallOpts, result *[]interface{}, method string, params ...interface{}) error {
	return _MToken.Contract.MTokenCaller.contract.Call(opts, result, method, params...)
}

// Transfer initiates a plain transaction to move funds to the contract, calling
// its default method if one is available.
func (_MToken *MTokenRaw) Transfer(opts *bind.TransactOpts) (*types.Transaction, error) {
	return _MToken.Contract.MTokenTransactor.contract.Transfer(opts)
}

// Transact invokes the (paid) contract method with params as input values.
func (_MToken *MTokenRaw) Transact(opts *bind.TransactOpts, method string, params ...interface{}) (*types.Transaction, error) {
	return _MToken.Contract.MTokenTransactor.contract.Transact(opts, method, params...)
}

// Call invokes the (constant) contract method with params as input values and
// sets the output to result. The result type might be a single field for simple
// returns, a slice of interfaces for anonymous returns and a struct for named
// returns.
func (_MToken *MTokenCallerRaw) Call(opts *bind.CallOpts, result *[]interface{}, method string, params ...interface{}) error {
	return _MToken.Contract.contract.Call(opts, result, method, params...)
}

// Transfer initiates a plain transaction to move funds to the contract, calling
// its default method if one is available.
func (_MToken *MTokenTransactorRaw) Transfer(opts *bind.TransactOpts) (*types.Transaction, error) {
	return _MToken.Contract.contract.Transfer(opts)
}

// Transact invokes the (paid) contract method with params as input values.
func (_MToken *MTokenTransactorRaw) Transact(opts *bind.TransactOpts, method string, params ...interface{}) (*types.Transaction, error) {
	return _MToken.Contract.contract.Transact(opts, method, params...)
}

// BalanceOf is a free data retrieval call binding the contract method 0x70a08231.
//
// Solidity: function balanceOf(address account) view returns(uint256)
func (_MToken *MTokenCaller) BalanceOf(opts *bind.CallOpts, account common.Address) (*big.Int, error) {
	var out []interface{}
	err := _MToken.contract.Call(opts, &out, "balanceOf", account)

	if err != nil {
		return *new(*big.Int), err
	}

	out0 := *abi.ConvertType(out[0], new(*big.Int)).(**big.Int)

	return out0, err

}

// BalanceOf is a free data retrieval call binding the contract method 0x70a08231.
//
// Solidity: function balanceOf(address account) view returns(uint256)
func (_MToken *MTokenSession) BalanceOf(account common.Address) (*big.Int, error) {
	return _MToken.Contract.BalanceOf(&_MToken.CallOpts, account)
}

// BalanceOf is a free data retrieval call binding the contract method 0x70a08231.
//
// Solidity: function balanceOf(address account) view returns(uint256)
func (_MToken *MTokenCallerSession) BalanceOf(account common.Address) (*big.Int, error) {
	return _MToken.Contract.BalanceOf(&_MToken.CallOpts, account)
}

// ExchangeRateStored is a free data retrieval call binding the contract method 0x182df0f5.
//
// Solidity: function exchangeRateStored() view returns(uint256)
func (_MToken *MTokenCaller) ExchangeRateStored(opts *bind.CallOpts) (*big.Int, error) {
	var out []interface{}
	err := _MToken.contract.Call(opts, &out, "exchangeRateStored")

	if err != nil {
		return *new(*big.Int), err
	}

	out0 := *abi.ConvertType(out[0], new(*big.Int)).(**big.Int)

	return out0, err

}

// ExchangeRateStored is a free data retrieval call binding the contract method 0x182df0f5.
//
// Solidity: function exchangeRateStored() view returns(uint256)
func (_MToken *MTokenSession) ExchangeRateStored() (*big.Int, error) {
	return _MToken.Contract.ExchangeRateStored(&_MToken.CallOpts)
}

// ExchangeRateStored is a free data retrieval call binding the contract method 0x182df0f5.
//
// Solidity: function exchangeRateStored() view returns(uint256)
func (_MToken *MTokenCallerSession) ExchangeRateStored() (*big.Int, error) {
	return _MToken.Contract.ExchangeRateStored(&_MToken.CallOpts)
}

// GetAccountSnapshot is a free data retrieval call binding the contract method 0xc37f68e2.
//
// Solidity: function getAccountSnapshot(address account) view returns(uint256, uint256, uint256, uint256)
func (_MToken *MTokenCaller) GetAccountSnapshot(opts *bind.CallOpts, account common.Address) (*big.Int, *big.Int, *big.Int, *big.Int, error) {
	var out []interface{}
	err := _MToken.contract.Call(opts, &out, "getAccountSnapshot", account)

	if err != nil {
		return *new(*big.Int), *new(*big.Int), *new(*big.Int), *new(*big.Int), err
	}

	out0 := *abi.ConvertType(out[0], new(*big.Int)).(**big.Int)
	out1 := *abi.ConvertType(out[1], new(*big.Int)).(**big.Int)
	out2 := *abi.ConvertType(out[2], new(*big.Int)).(**big.Int)
	out3 := *abi.ConvertType(out[3], new(*big.Int)).(**big.Int)

	return out0, out1, out2, out3, err

}

// GetAccountSnapshot is a free data retrieval call binding the contract method 0xc37f68e2.
//
// Solidity: function getAccountSnapshot(address account) view returns(uint256, uint256, uint256, uint256)
func (_MToken *MTokenSession) GetAccountSnapshot(account common.Address) (*big.Int, *big.Int, *big.Int, *big.Int, error) {
	return _MToken.Contract.GetAccountSnapshot(&_MToken.CallOpts, account)
}

// GetAccountSnapshot is a free data retrieval call binding the contract method 0xc37f68e2.
//
// Solidity: function getAccountSnapshot(address account) view returns(uint256, uint256, uint256, uint256)
func (_MToken *MTokenCallerSession) GetAccountSnapshot(account common.Address) (*big.Int, *big.Int, *big.Int, *big.Int, error) {
	return _MToken.Contract.GetAccountSnapshot(&_MToken.CallOpts, account)
}

// Underlying is a free data retrieval call binding the contract method 0x6f307dc3.
//
// Solidity: function underlying() view returns(address)
func (_MToken *MTokenCaller) Underlying(opts *bind.CallOpts) (common.Address, error) {
	var out []interface{}
	err := _MToken.contract.Call(opts, &out, "underlying")

	if err != nil {
		return *new(common.Address), err
	}

	out0 := *abi.ConvertType(out[0], new(common.Address)).(*common.Address)

	return out0, err

}

// Underlying is a free data retrieval call binding the contract method 0x6f307dc3.
//
// Solidity: function underlying() view returns(address)
func (_MToken *MTokenSession) Underlying() (common.Address, error) {
	return _MToken.Contract.Underlying(&_MToken.CallOpts)
}

// Underlying is a free data retrieval call binding the contract method 0x6f307dc3.
//
// Solidity: function underlying() view returns(address)
func (_MToken *MTokenCallerSession) Underlying() (common.Address, error) {
	return _MToken.Contract.Underlying(&_MToken.CallOpts)
}
