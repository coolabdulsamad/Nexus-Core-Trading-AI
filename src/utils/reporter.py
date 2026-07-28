"""
src/utils/reporter.py
Analysis & reporting: end-of-day reports, capital analysis, trade stats.
Feeds Telegram and (later) the frontend API.
"""
from datetime import datetime
from collections import defaultdict
from src.utils.telegram import send_telegram
from src.utils.logger import setup_logger

logger = setup_logger("Reporter", "logs/reporter.log")


def trade_stats(trades: list) -> dict:
    if not trades:
        return {'n': 0, 'wins': 0, 'losses': 0, 'win_rate': 0.0,
                'pnl': 0.0, 'avg_win': 0.0, 'avg_loss': 0.0, 'profit_factor': 0.0,
                'best': None, 'worst': None, 'by_symbol': {}, 'by_reason': {}}
    wins = [t for t in trades if t.get('pnl', 0) > 0]
    losses = [t for t in trades if t.get('pnl', 0) <= 0]
    pnl = sum(t.get('pnl', 0) for t in trades)
    gross_w = sum(t['pnl'] for t in wins)
    gross_l = abs(sum(t['pnl'] for t in losses))

    by_symbol = defaultdict(lambda: {'n': 0, 'pnl': 0.0})
    by_reason = defaultdict(lambda: {'n': 0, 'pnl': 0.0})
    for t in trades:
        by_symbol[t.get('symbol', '?')]['n'] += 1
        by_symbol[t.get('symbol', '?')]['pnl'] += t.get('pnl', 0)
        by_reason[t.get('reason', t.get('exit_reason', '?'))]['n'] += 1
        by_reason[t.get('reason', t.get('exit_reason', '?'))]['pnl'] += t.get('pnl', 0)

    return {
        'n': len(trades), 'wins': len(wins), 'losses': len(losses),
        'win_rate': len(wins) / len(trades) * 100,
        'pnl': pnl,
        'avg_win': gross_w / len(wins) if wins else 0.0,
        'avg_loss': -gross_l / len(losses) if losses else 0.0,
        'profit_factor': gross_w / gross_l if gross_l > 0 else float('inf'),
        'best': max(trades, key=lambda t: t.get('pnl', 0)),
        'worst': min(trades, key=lambda t: t.get('pnl', 0)),
        'by_symbol': dict(by_symbol), 'by_reason': dict(by_reason),
    }


def format_eod_report(trades: list, equity: float, day_start_equity: float,
                      open_positions: dict = None) -> str:
    s = trade_stats(trades)
    day_pnl = equity - day_start_equity
    day_pct = day_pnl / day_start_equity * 100 if day_start_equity else 0
    lines = [
        "📊 END-OF-DAY REPORT",
        f"📅 {datetime.now().strftime('%Y-%m-%d')}",
        "━━━━━━━━━━━━━━━━━━",
        f"💰 Capital: ${equity:,.2f} ({day_pct:+.2f}% today, {day_pnl:+,.2f}$)",
        f"🎯 Trades: {s['n']} | Win rate: {s['win_rate']:.1f}% | PF: {s['profit_factor']:.2f}",
        f"📈 Avg win: ${s['avg_win']:,.2f} | Avg loss: ${s['avg_loss']:,.2f}",
    ]
    if s['best']:
        lines.append(f"🏆 Best: {s['best'].get('symbol','?')} ${s['best'].get('pnl',0):+,.2f}")
    if s['worst']:
        lines.append(f"💀 Worst: {s['worst'].get('symbol','?')} ${s['worst'].get('pnl',0):+,.2f}")
    if s['by_symbol']:
        lines.append("— Per symbol —")
        for sym, r in sorted(s['by_symbol'].items(), key=lambda x: -x[1]['pnl']):
            lines.append(f"  {sym}: {r['n']} trades, ${r['pnl']:+,.2f}")
    if s['by_reason']:
        lines.append("— Exit reasons —")
        for reason, r in sorted(s['by_reason'].items(), key=lambda x: x[1]['pnl']):
            lines.append(f"  {reason}: {r['n']}x, ${r['pnl']:+,.2f}")
    if open_positions:
        open_syms = [k for k, v in open_positions.items() if v is not None]
        lines.append(f"— Open overnight: {', '.join(open_syms) if open_syms else 'none'} —")
    return "\n".join(lines)


def format_capital_update(equity: float, day_start_equity: float,
                          peak_equity: float, positions_open: int) -> str:
    day_pct = (equity - day_start_equity) / day_start_equity * 100 if day_start_equity else 0
    dd = (peak_equity - equity) / peak_equity * 100 if peak_equity else 0
    return (f"💓 Equity ${equity:,.2f} ({day_pct:+.2f}% today) | "
            f"DD from peak: {dd:.2f}% | Open positions: {positions_open}")


def send_eod_report(trades, equity, day_start_equity, open_positions=None):
    msg = format_eod_report(trades, equity, day_start_equity, open_positions)
    logger.info("EOD report sent.")
    return send_telegram(msg, kind='report')
