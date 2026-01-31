from flask import Flask, request, jsonify
from datetime import datetime, timedelta
import time
import json
import threading
import os

app = Flask(__name__)

print("=" * 60)
print("MT5 TRADE COPIER SERVER - FINAL FIX v6.0")
print("=" * 60)

# Storage structures
signals_queue = {}  # master_id -> list of signals with unique IDs
slaves = {}  # master_id -> dict of slave_id -> slave_info
slave_processed_signals = {}  # slave_id -> set of processed signal IDs


def cleanup_old_signals():
    """Clean up signals older than 10 minutes"""
    while True:
        time.sleep(60)
        now = datetime.now()

        for master_id in list(signals_queue.keys()):
            if master_id in signals_queue:
                # Keep only signals from last 10 minutes
                original_count = len(signals_queue[master_id])
                signals_queue[master_id] = [
                    s
                    for s in signals_queue[master_id]
                    if (
                        now
                        - datetime.fromisoformat(
                            s.get("server_timestamp", "2000-01-01")
                        )
                    ).seconds
                    < 600
                ]

                cleaned_count = original_count - len(signals_queue[master_id])
                if cleaned_count > 0:
                    print(f"🗑️ Cleaned {cleaned_count} old signals for {master_id}")

                if not signals_queue[master_id]:
                    del signals_queue[master_id]


cleanup_thread = threading.Thread(target=cleanup_old_signals, daemon=True)
cleanup_thread.start()


# ==================== ROUTES ====================
@app.route("/", methods=["GET"])
def home():
    return jsonify(
        {
            "status": "MT5 Trade Copier Server - Final Fix v6.0",
            "version": "6.0",
            "fixes": [
                "Optional symbol field for DELETE_ORDER",
                "Modify signals always create new entries",
                "Unique signal IDs with timestamps",
                "Better duplicate prevention",
                "10-minute signal retention",
                "FIXED: Returns oldest unprocessed signal first",
                "FIXED: Only marks returned signal as processed",
            ],
            "stats": {
                "active_masters": len(signals_queue),
                "total_signals": sum(len(sigs) for sigs in signals_queue.values()),
                "registered_slaves": sum(len(s) for s in slaves.values()),
            },
        }
    )


@app.route("/upload_signal", methods=["POST"])
def upload_signal():
    """Master EA sends trade signal"""
    try:
        data = request.get_json()

        if not data:
            return jsonify({"error": "No data provided"}), 400

        print(f"\n📥 RECEIVED SIGNAL: {json.dumps(data, indent=2)}")

        # Required fields (symbol is optional for DELETE_ORDER)
        master_id = data.get("master_id")
        action = data.get("action")
        master_ticket = data.get("master_ticket")

        if not master_id or not action or master_ticket is None:
            return (
                jsonify({"error": "Missing master_id, action, or master_ticket"}),
                400,
            )

        # Validate action
        valid_actions = [
            "NEW_TRADE",
            "NEW_PENDING",
            "MODIFY_TRADE",
            "MODIFY_PENDING",
            "CLOSE_TRADE",
            "DELETE_ORDER",
            "INIT_TEST",
            "HEARTBEAT",
        ]

        if action not in valid_actions:
            return jsonify({"error": f"Invalid action: {action}"}), 400

        # Symbol is required for all actions except DELETE_ORDER, INIT_TEST, HEARTBEAT
        if action not in ["DELETE_ORDER", "INIT_TEST", "HEARTBEAT"]:
            if "symbol" not in data:
                return jsonify({"error": f"Missing symbol for action {action}"}), 400

        # Skip test and heartbeat signals from queue
        if action in ["INIT_TEST", "HEARTBEAT"]:
            print(f"ℹ️ {action} from {master_id}")
            return jsonify({"status": "acknowledged", "action": action})

        # Add server timestamp
        server_time = datetime.now().isoformat()
        data["server_timestamp"] = server_time

        # Create unique signal ID (ticket + action + timestamp)
        signal_id = f"{master_ticket}_{action}_{int(datetime.now().timestamp() * 1000)}"
        data["signal_id"] = signal_id

        # Initialize if needed
        if master_id not in signals_queue:
            signals_queue[master_id] = []

        # For MODIFY actions, ALWAYS add new signal (don't update existing)
        if action in ["MODIFY_TRADE", "MODIFY_PENDING"]:
            signals_queue[master_id].append(data)
            print(
                f"✅ Added NEW modification signal: {action} for ticket {master_ticket}"
            )
            print(f"   Signal ID: {signal_id}")

        # For DELETE_ORDER and CLOSE_TRADE, remove any pending NEW/MODIFY signals for same ticket
        elif action in ["DELETE_ORDER", "CLOSE_TRADE"]:
            # Remove any unprocessed signals for this ticket
            removed_count = len(signals_queue[master_id])
            signals_queue[master_id] = [
                s
                for s in signals_queue[master_id]
                if s.get("master_ticket") != master_ticket
            ]
            removed_count = removed_count - len(signals_queue[master_id])

            # Add the close/delete signal
            signals_queue[master_id].append(data)
            print(f"✅ Added {action} signal for ticket {master_ticket}")
            if removed_count > 0:
                print(f"   Removed {removed_count} pending signals for this ticket")

        # For NEW_TRADE and NEW_PENDING
        else:
            # Check if identical signal already exists (prevent true duplicates)
            is_duplicate = False
            for sig in signals_queue[master_id]:
                if (
                    sig.get("master_ticket") == master_ticket
                    and sig.get("action") == action
                    and sig.get("symbol") == data.get("symbol")
                ):
                    is_duplicate = True
                    print(
                        f"⚠️ Duplicate {action} signal for ticket {master_ticket} - skipping"
                    )
                    break

            if not is_duplicate:
                signals_queue[master_id].append(data)
                print(f"✅ Added {action} signal for ticket {master_ticket}")

        # Keep queue size manageable (max 50 signals per master)
        if len(signals_queue[master_id]) > 50:
            removed = len(signals_queue[master_id]) - 50
            signals_queue[master_id] = signals_queue[master_id][-50:]
            print(f"⚠️ Queue limit reached - removed {removed} oldest signals")

        print(
            f"📊 Current queue for {master_id}: {len(signals_queue[master_id])} signals"
        )

        return jsonify(
            {
                "status": "received",
                "master_id": master_id,
                "action": action,
                "ticket": master_ticket,
                "signal_id": signal_id,
            }
        )

    except Exception as e:
        print(f"❌ ERROR in upload_signal: {e}")
        import traceback

        traceback.print_exc()
        return jsonify({"error": str(e)}), 500


@app.route("/register_slave", methods=["POST"])
def register_slave():
    """Slave EA registers to copy from master"""
    try:
        data = request.get_json()

        if not data:
            return jsonify({"error": "No data provided"}), 400

        slave_id = data.get("slave_id")
        master_id = data.get("master_id")

        if not slave_id or not master_id:
            return jsonify({"error": "Missing slave_id or master_id"}), 400

        # Initialize structures
        if master_id not in slaves:
            slaves[master_id] = {}

        slaves[master_id][slave_id] = {
            "registered_at": datetime.now().isoformat(),
            "last_poll": datetime.now().isoformat(),
        }

        # Initialize processed signals set
        if slave_id not in slave_processed_signals:
            slave_processed_signals[slave_id] = set()

        print(f"✅ SLAVE REGISTERED: {slave_id} → {master_id}")

        return jsonify(
            {
                "status": "registered",
                "slave_id": slave_id,
                "master_id": master_id,
                "timestamp": datetime.now().isoformat(),
            }
        )

    except Exception as e:
        print(f"❌ ERROR in register_slave: {e}")
        return jsonify({"error": str(e)}), 500


@app.route("/slave_poll", methods=["GET"])
def slave_poll():
    """Slave EA polls for new signals - FIXED VERSION"""
    try:
        slave_id = request.args.get("slave_id")
        master_id = request.args.get("master_id")

        if not slave_id or not master_id:
            return jsonify({"error": "Missing slave_id or master_id"}), 400

        # Check if slave is registered
        if master_id not in slaves or slave_id not in slaves[master_id]:
            # Auto-register
            if master_id not in slaves:
                slaves[master_id] = {}
            slaves[master_id][slave_id] = {
                "registered_at": datetime.now().isoformat(),
                "last_poll": datetime.now().isoformat(),
            }

        # Update last poll time
        slaves[master_id][slave_id]["last_poll"] = datetime.now().isoformat()

        # Get unprocessed signals
        signal_to_return = None

        if master_id in signals_queue:
            if slave_id not in slave_processed_signals:
                slave_processed_signals[slave_id] = set()

            # FIXED: Find OLDEST unprocessed signal (FIFO - First In First Out)
            for signal in signals_queue[master_id]:
                signal_id = signal.get("signal_id")

                if signal_id and signal_id not in slave_processed_signals[slave_id]:
                    # Found the oldest unprocessed signal
                    signal_to_return = signal

                    # Mark THIS signal as processed
                    slave_processed_signals[slave_id].add(signal_id)

                    print(
                        f"📤 Sending to {slave_id}: {signal.get('action')} ticket {signal.get('master_ticket')}"
                    )
                    print(f"   Signal ID: {signal_id}")

                    # Only send ONE signal at a time
                    break

            # Clean up processed signals set (keep last 200)
            if len(slave_processed_signals[slave_id]) > 200:
                signal_list = list(slave_processed_signals[slave_id])
                slave_processed_signals[slave_id] = set(signal_list[-200:])

        # Return the signal (or empty list if none)
        signals_to_return = [signal_to_return] if signal_to_return else []

        if signals_to_return:
            print(f"✅ Returned 1 signal to {slave_id}")

        return jsonify(
            {
                "status": "ok",
                "signals": signals_to_return,
                "count": len(signals_to_return),
                "timestamp": datetime.now().isoformat(),
            }
        )

    except Exception as e:
        print(f"❌ ERROR in slave_poll: {e}")
        import traceback

        traceback.print_exc()
        return jsonify({"error": str(e)}), 500


@app.route("/clear_signals", methods=["POST"])
def clear_signals():
    """Clear signals and processed tracking"""
    try:
        data = request.get_json()
        master_id = data.get("master_id")
        slave_id = data.get("slave_id")

        cleared = {}

        if slave_id:
            # Clear processed signals for slave
            if slave_id in slave_processed_signals:
                count = len(slave_processed_signals[slave_id])
                slave_processed_signals[slave_id].clear()
                print(f"🗑️ Cleared {count} processed signals for slave {slave_id}")
                cleared["slave_processed"] = count

        if master_id:
            # Clear signal queue for master
            if master_id in signals_queue:
                count = len(signals_queue[master_id])
                del signals_queue[master_id]
                print(f"🗑️ Cleared {count} signals for master {master_id}")
                cleared["master_queue"] = count

        if cleared:
            return jsonify({"status": "cleared", **cleared})
        else:
            return jsonify({"status": "nothing_to_clear"})

    except Exception as e:
        return jsonify({"error": str(e)}), 500


@app.route("/status", methods=["GET"])
def status():
    """Get detailed server status"""
    master_stats = {}

    for master_id, signals in signals_queue.items():
        master_stats[master_id] = {
            "pending_signals": len(signals),
            "latest_signal": signals[-1] if signals else None,
            "oldest_signal": signals[0] if signals else None,
            "connected_slaves": list(slaves.get(master_id, {}).keys()),
            "signals_by_action": {},
        }

        # Count by action type
        for sig in signals:
            action = sig.get("action")
            if action:
                master_stats[master_id]["signals_by_action"][action] = (
                    master_stats[master_id]["signals_by_action"].get(action, 0) + 1
                )

        # Show all signal tickets for debugging
        signal_tickets = [
            f"{s.get('master_ticket')}({s.get('action')})" for s in signals
        ]
        master_stats[master_id]["signal_list"] = signal_tickets

    # Slave stats
    slave_stats = {}
    for slave_id, processed in slave_processed_signals.items():
        slave_stats[slave_id] = {
            "processed_count": len(processed),
            "last_10_processed": list(processed)[-10:] if processed else [],
        }

    return jsonify(
        {
            "status": "running",
            "version": "6.0",
            "timestamp": datetime.now().isoformat(),
            "masters": master_stats,
            "slaves": slave_stats,
            "total_pending_signals": sum(len(sigs) for sigs in signals_queue.values()),
        }
    )


# ==================== MAIN ====================
if __name__ == "__main__":
    print("\n🔧 FIXES IN VERSION 6.0:")
    print("   ✅ 1. Optional 'symbol' field (fixes DELETE_ORDER)")
    print("   ✅ 2. MODIFY actions always create NEW signals")
    print("   ✅ 3. Unique signal IDs with timestamps")
    print("   ✅ 4. Better duplicate prevention")
    print("   ✅ 5. 10-minute signal retention")
    print("   ✅ 6. DELETE/CLOSE cleans up previous signals")
    print("   ✅ 7. FIFO processing (oldest signal first)")
    print("   ✅ 8. Only marks processed signal (not all)")

    # Get port from environment variable (Railway sets this) or default to 8080
    port = int(os.environ.get("PORT", 8080))
    host = "0.0.0.0"  # Bind to all interfaces for Railway

    print(f"\n🌐 Server URL: http://{host}:{port}")
    print("\n📋 Quick Commands:")
    print(f"   curl http://YOUR_RAILWAY_URL/status")
    print(
        f'   curl -X POST http://YOUR_RAILWAY_URL/clear_signals -H "Content-Type: application/json" -d \'{{"master_id":"TRADER_001","slave_id":"SLAVE_001"}}\''
    )
    print("\n" + "=" * 60 + "\n")

    app.run(host=host, port=port, debug=False)
