// ساخت گراف پیش نیاز (Precedence Graph)
function buildPrecedenceGraph(schedule) {
    const graph = {};
    const abortedTransactions = new Set();
    
    // شناسایی تراکنش‌های abort شده
    schedule.forEach(([tid, op]) => {
        if (op === 'A') {
            abortedTransactions.add(tid);
        }
    });
    
    // حذف عملیات تراکنش‌های abort شده
    const validSchedule = schedule.filter(([tid, op]) => {
        return !abortedTransactions.has(tid) || op === 'A';
    });
    
    // ساخت گراف فقط از تراکنش‌های commit شده یا در حال اجرا
    validSchedule.forEach(([t]) => { 
        if (!graph[t]) graph[t] = new Set(); 
    });

    for (let i = 0; i < validSchedule.length; i++) {
        const [t1, op1, data1] = validSchedule[i];
        // نادیده گرفتن عملیات Commit و Abort در بررسی conflict
        if (op1 === 'C' || op1 === 'A') continue;
        
        for (let j = i + 1; j < validSchedule.length; j++) {
            const [t2, op2, data2] = validSchedule[j];
            // نادیده گرفتن عملیات Commit و Abort در بررسی conflict
            if (op2 === 'C' || op2 === 'A') continue;
            
            if (t1 !== t2 && data1 === data2 && data1 !== '') {
                // Conflict: Write-Read, Read-Write, Write-Write
                if (op1 === 'W' || op2 === 'W') {
                    graph[t1].add(t2);
                }
            }
        }
    }
    return graph;
}

// چک کردن حلقه در گراف با DFS
function hasCycle(graph) {
    const visited = new Set();
    const recStack = new Set();

    function dfs(node) {
        visited.add(node);
        recStack.add(node);
        for (const neighbor of graph[node]) {
            if (!visited.has(neighbor)) {
                if (dfs(neighbor)) return true;
            } else if (recStack.has(neighbor)) {
                return true;
            }
        }
        recStack.delete(node);
        return false;
    }

    for (const node in graph) {
        if (!visited.has(node)) {
            if (dfs(node)) return true;
        }
    }
    return false;
}

// بررسی اینکه آیا تراکنش‌ها به درستی پایان یافته‌اند
function validateTransactionEndings(schedule) {
    const transactions = {};
    const warnings = [];
    
    schedule.forEach(([tid, op]) => {
        if (!transactions[tid]) {
            transactions[tid] = { hasCommit: false, hasAbort: false, hasOtherOps: false };
        }
        
        if (op === 'C') transactions[tid].hasCommit = true;
        else if (op === 'A') transactions[tid].hasAbort = true;
        else transactions[tid].hasOtherOps = true;
    });
    
    for (const [tid, state] of Object.entries(transactions)) {
        if (state.hasCommit && state.hasAbort) {
            warnings.push(`هشدار: تراکنش ${tid} هم Commit و هم Abort دارد!`);
        } else if (!state.hasCommit && !state.hasAbort && state.hasOtherOps) {
            warnings.push(`هشدار: تراکنش ${tid} بدون Commit یا Abort است!`);
        }
    }
    
    return warnings;
}

// تابع اصلی برای بررسی سریال‌پذیری
function checkConflictSerializability(schedule) {
    const warnings = validateTransactionEndings(schedule);
    const graph = buildPrecedenceGraph(schedule);
    const isSerializable = !hasCycle(graph);
    
    return {
        isSerializable,
        warnings,
        graph: convertGraphToObject(graph)
    };
}

// تبدیل گراف با Set به Object برای نمایش بهتر
function convertGraphToObject(graph) {
    const result = {};
    for (const [node, neighbors] of Object.entries(graph)) {
        result[node] = Array.from(neighbors);
    }
    return result;
}

// مثال استفاده در Node.js
if (typeof module !== 'undefined' && module.exports) {
    module.exports = {
        buildPrecedenceGraph,
        hasCycle,
        validateTransactionEndings,
        checkConflictSerializability,
        convertGraphToObject
    };
}

// برای تست در Node.js
if (require.main === module) {
    // مثال 1: Schedule سریال‌پذیر
    const schedule1 = [
        ['T1', 'R', 'x'],
        ['T2', 'R', 'x'],
        ['T1', 'W', 'x'],
        ['T2', 'C', ''],
        ['T1', 'C', '']
    ];
    
    console.log('=== مثال 1: Schedule سریال‌پذیر ===');
    console.log('Schedule:', schedule1);
    const result1 = checkConflictSerializability(schedule1);
    console.log('نتیجه:', result1.isSerializable ? 'سریال‌پذیر است ✓' : 'سریال‌پذیر نیست ✗');
    console.log('گراف پیش نیاز:', result1.graph);
    if (result1.warnings.length > 0) {
        console.log('هشدارها:', result1.warnings);
    }
    console.log('');
    
    // مثال 2: Schedule غیر سریال‌پذیر
    const schedule2 = [
        ['T1', 'R', 'x'],
        ['T2', 'W', 'x'],
        ['T2', 'R', 'y'],
        ['T1', 'W', 'y'],
        ['T1', 'C', ''],
        ['T2', 'C', '']
    ];
    
    console.log('=== مثال 2: Schedule غیر سریال‌پذیر ===');
    console.log('Schedule:', schedule2);
    const result2 = checkConflictSerializability(schedule2);
    console.log('نتیجه:', result2.isSerializable ? 'سریال‌پذیر است ✓' : 'سریال‌پذیر نیست ✗');
    console.log('گراف پیش نیاز:', result2.graph);
    if (result2.warnings.length > 0) {
        console.log('هشدارها:', result2.warnings);
    }
    console.log('');
    
    // مثال 3: Schedule با Abort
    const schedule3 = [
        ['T1', 'R', 'x'],
        ['T2', 'W', 'x'],
        ['T1', 'A', ''],
        ['T2', 'C', '']
    ];
    
    console.log('=== مثال 3: Schedule با Abort ===');
    console.log('Schedule:', schedule3);
    const result3 = checkConflictSerializability(schedule3);
    console.log('نتیجه:', result3.isSerializable ? 'سریال‌پذیر است ✓' : 'سریال‌پذیر نیست ✗');
    console.log('گراف پیش نیاز:', result3.graph);
    if (result3.warnings.length > 0) {
        console.log('هشدارها:', result3.warnings);
    }
}

