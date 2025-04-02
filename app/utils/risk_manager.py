import pandas as pd
import numpy as np
import logging
from typing import Dict, Optional, Tuple, Union
from decimal import Decimal

logger = logging.getLogger(__name__)

class RiskManager:
    """Manages position sizing and risk parameters for trading strategies."""

    def __init__(self, max_risk_percent: float = 1.0, risk_reward_ratio: float = 2.0, max_positions: int = 3):
        """
        Initialize the risk manager.
        
        Args:
            max_risk_percent: Maximum percentage of account to risk per trade (default: 1%)
            risk_reward_ratio: Target ratio of reward to risk (default: 2.0)
            max_positions: Maximum number of concurrent positions (default: 3)
        """
        if not 0 < max_risk_percent <= 5:
            raise ValueError("max_risk_percent must be between 0 and 5")
        if risk_reward_ratio <= 0:
            raise ValueError("risk_reward_ratio must be positive")
        if max_positions <= 0:
            raise ValueError("max_positions must be positive")
            
        self.max_risk_percent = max_risk_percent
        self.risk_reward_ratio = risk_reward_ratio
        self.max_positions = max_positions
        self.open_positions: Dict[str, Dict] = {}
        logger.info(f"RiskManager initialized: max_risk={max_risk_percent}%, risk_reward={risk_reward_ratio}, max_positions={max_positions}")

    def calculate_position_size(self, account_balance: float, entry_price: float, stop_loss: float, 
                                symbol: str, max_position_size: Optional[float] = None) -> float:
        """
        Calculate appropriate position size based on account balance and risk parameters.
        
        Args:
            account_balance: Current account balance in USDT
            entry_price: Planned entry price
            stop_loss: Planned stop loss price
            symbol: Trading pair symbol (e.g., 'BTCUSDT')
            max_position_size: Maximum position size in base currency units
            
        Returns:
            Quantity to buy/sell in base currency units
        """
        if not all(isinstance(x, (int, float)) and x > 0 for x in [account_balance, entry_price]):
            raise ValueError("account_balance and entry_price must be positive numbers")
        if not isinstance(stop_loss, (int, float)) or stop_loss <= 0:
            raise ValueError("stop_loss must be a positive number")
        if stop_loss >= entry_price:
            raise ValueError("stop_loss must be lower than entry_price for long positions")
            
        try:
            # Calculate risk amount in USDT
            risk_amount = account_balance * (self.max_risk_percent / 100)
            logger.debug(f"Risk amount for {symbol}: {risk_amount:.2f} USDT")
            
            # Calculate position size based on risk
            price_risk = abs(entry_price - stop_loss)
            position_risk_percent = price_risk / entry_price
            position_size_usd = risk_amount / position_risk_percent
            
            # Convert to quantity in base currency
            quantity = position_size_usd / entry_price
            
            # Limit by max position size if specified
            if max_position_size and quantity > max_position_size:
                logger.info(f"Position size reduced from {quantity:.6f} to max {max_position_size:.6f} for {symbol}")
                quantity = max_position_size
                
            # Round to appropriate precision (typically 5-8 decimal places for crypto)
            quantity = self._adjust_precision(quantity, symbol)
            
            logger.info(f"Calculated position size for {symbol}: {quantity} units (value: {quantity * entry_price:.2f} USDT)")
            return quantity
            
        except Exception as e:
            logger.error(f"Error calculating position size for {symbol}: {str(e)}")
            # Return a safe minimum value in case of error
            return 0.0
    
    def calculate_take_profit(self, entry_price: float, stop_loss: float, custom_ratio: Optional[float] = None) -> float:
        """
        Calculate take profit level based on risk-reward ratio.
        
        Args:
            entry_price: Entry price
            stop_loss: Stop loss price
            custom_ratio: Override default risk-reward ratio
            
        Returns:
            Take profit price
        """
        ratio = custom_ratio if custom_ratio is not None else self.risk_reward_ratio
        if ratio <= 0:
            raise ValueError("risk_reward_ratio must be positive")
            
        risk = abs(entry_price - stop_loss)
        take_profit = entry_price + (risk * ratio)
        logger.debug(f"Calculated take profit: {take_profit:.2f} (entry: {entry_price:.2f}, SL: {stop_loss:.2f}, ratio: {ratio})")
        return take_profit
    
    def calculate_trailing_stop(self, entry_price: float, current_price: float, initial_stop: float, 
                               activation_percent: float = 1.0, trail_percent: float = 0.5) -> float:
        """
        Calculate trailing stop level.
        
        Args:
            entry_price: Original entry price
            current_price: Current market price
            initial_stop: Initial stop loss price
            activation_percent: Percentage gain needed to activate trailing (default: 1%)
            trail_percent: Percentage of price move to trail by (default: 0.5%)
            
        Returns:
            Updated stop loss price
        """
        # For long positions
        if entry_price < current_price:
            profit_percent = (current_price - entry_price) / entry_price * 100
            
            # Only activate trailing stop once we've hit activation threshold
            if profit_percent >= activation_percent:
                # Calculate new stop based on trail percentage
                potential_stop = current_price * (1 - trail_percent / 100)
                
                # Only update if it would raise the stop
                if potential_stop > initial_stop:
                    logger.debug(f"Trailing stop updated: {initial_stop:.2f} -> {potential_stop:.2f}")
                    return potential_stop
        
        # Return original stop if conditions not met
        return initial_stop
    
    def track_position(self, symbol: str, quantity: float, entry_price: float, 
                      stop_loss: float, take_profit: float) -> None:
        """
        Track a new open position.
        
        Args:
            symbol: Trading pair symbol
            quantity: Position size in base currency
            entry_price: Entry price
            stop_loss: Stop loss price
            take_profit: Take profit price
        """
        if len(self.open_positions) >= self.max_positions:
            logger.warning(f"Maximum positions reached ({self.max_positions}), but attempting to add {symbol}")
            
        position = {
            "quantity": quantity,
            "entry_price": entry_price,
            "stop_loss": stop_loss,
            "take_profit": take_profit,
            "current_price": entry_price,
            "unrealized_pnl": 0.0,
            "risk_amount": quantity * abs(entry_price - stop_loss)
        }
        
        self.open_positions[symbol] = position
        logger.info(f"Tracking new position for {symbol}: {quantity} units at {entry_price}")
    
    def update_position(self, symbol: str, current_price: float) -> Optional[str]:
        """
        Update an existing position with current price and check for exit conditions.
        
        Args:
            symbol: Trading pair symbol
            current_price: Current market price
            
        Returns:
            Exit signal type ('stop_loss', 'take_profit', or None)
        """
        if symbol not in self.open_positions:
            logger.warning(f"Cannot update non-existent position for {symbol}")
            return None
            
        position = self.open_positions[symbol]
        position["current_price"] = current_price
        
        # Calculate unrealized P&L
        entry_price = position["entry_price"]
        quantity = position["quantity"]
        price_diff = current_price - entry_price
        position["unrealized_pnl"] = price_diff * quantity
        
        # Check for exit conditions
        if current_price <= position["stop_loss"]:
            logger.info(f"Stop loss triggered for {symbol} at {current_price}")
            return "stop_loss"
        if current_price >= position["take_profit"]:
            logger.info(f"Take profit triggered for {symbol} at {current_price}")
            return "take_profit"
        
        # Update trailing stop if needed
        position["stop_loss"] = self.calculate_trailing_stop(
            entry_price, current_price, position["stop_loss"]
        )
        
        return None
    
    def close_position(self, symbol: str) -> Dict:
        """
        Close and remove a tracked position.
        
        Args:
            symbol: Trading pair symbol
            
        Returns:
            Position details including final P&L
        """
        if symbol not in self.open_positions:
            logger.warning(f"Cannot close non-existent position for {symbol}")
            return {}
            
        position = self.open_positions.pop(symbol)
        logger.info(f"Closed position for {symbol}: P&L = {position['unrealized_pnl']:.2f} USDT")
        return position
    
    def get_total_risk_exposure(self) -> float:
        """
        Calculate total current risk exposure across all positions.
        
        Returns:
            Total risk amount in USDT
        """
        total_risk = sum(p["risk_amount"] for p in self.open_positions.values())
        logger.debug(f"Total risk exposure: {total_risk:.2f} USDT across {len(self.open_positions)} positions")
        return total_risk
    
    def _adjust_precision(self, quantity: float, symbol: str) -> float:
        """
        Adjust quantity precision based on symbol requirements.
        This is a simplified version - in production, you'd query exchange for actual precision.
        """
        # Default precisions for common symbols
        precision_map = {
            "BTCUSDT": 5,
            "ETHUSDT": 4,
            "BNBUSDT": 2,
            "ADAUSDT": 0,
            "DOGEUSDT": 0,
            "XRPUSDT": 1
        }
        
        # Default precision if symbol not found
        precision = precision_map.get(symbol, 2)
        
        # Round to appropriate precision
        factor = 10 ** precision
        return float(Decimal(quantity * factor).quantize(Decimal('1')) / factor)