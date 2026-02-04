<!DOCTYPE html>
<html lang="{{ str_replace('_', '-', app()->getLocale()) }}">

<head>
    <meta charset="utf-8">
    <meta name="viewport" content="width=device-width, initial-scale=1">
    <title>Processing Status | Annual Pass Reconciler</title>
    <script src="https://cdn.tailwindcss.com"></script>
    <link rel="preconnect" href="https://fonts.bunny.net">
    <link href="https://fonts.bunny.net/css?family=inter:400,600,700,800&display=swap" rel="stylesheet" />
    <style>
        body {
            font-family: 'Inter', sans-serif;
            background: linear-gradient(135deg, #f8fafc 0%, #e8f4f8 50%, #f0f5ff 100%);
            min-height: 100vh;
        }

        .main-header {
            background: linear-gradient(90deg, #4a90a4 0%, #6b8cce 100%);
            -webkit-background-clip: text;
            -webkit-text-fill-color: transparent;
        }

        .status-card {
            background: rgba(255, 255, 255, 0.9);
            backdrop-filter: blur(10px);
            border: 1px solid rgba(74, 144, 164, 0.2);
            box-shadow: 0 8px 32px rgba(74, 144, 164, 0.1);
        }

        .progress-bar {
            background: linear-gradient(90deg, #6b9eb8 0%, #8ba4c9 100%);
        }

        @keyframes pulse {

            0%,
            100% {
                opacity: 1;
            }

            50% {
                opacity: 0.5;
            }
        }

        .animate-pulse-slow {
            animation: pulse 2s cubic-bezier(0.4, 0, 0.6, 1) infinite;
        }
    </style>
    @if ($status === 'processing')
        <meta http-equiv="refresh" content="5">
    @endif
</head>

<body class="text-gray-800 p-6 flex flex-col items-center justify-center min-h-screen">

    <div class="w-full max-w-lg">

        <div class="status-card rounded-2xl shadow-xl overflow-hidden p-10 text-center">

            <h1 class="main-header text-3xl font-extrabold mb-8">Processing Pipeline</h1>

            @if ($status === 'processing')
                <div class="mb-8">
                    <div
                        class="w-24 h-24 bg-blue-50 rounded-full flex items-center justify-center mx-auto mb-6 animate-pulse-slow border-4 border-blue-100">
                        <svg class="w-12 h-12 text-[#6b9eb8] animate-spin" fill="none" viewBox="0 0 24 24">
                            <circle class="opacity-25" cx="12" cy="12" r="10" stroke="currentColor"
                                stroke-width="4"></circle>
                            <path class="opacity-75" fill="currentColor"
                                d="M4 12a8 8 0 018-8V0C5.373 0 0 5.373 0 12h4zm2 5.291A7.962 7.962 0 014 12H0c0 3.042 1.135 5.824 3 7.938l3-2.647z">
                            </path>
                        </svg>
                    </div>

                    <h2 class="text-2xl font-bold text-[#2d3748] mb-2">Running...</h2>
                    <p class="text-[#5a6a7a]">Processing transactions, slicing, merging, and reconciling.</p>

                    <div class="w-full bg-gray-200 rounded-full h-2.5 mt-6 mb-2">
                        <div class="progress-bar h-2.5 rounded-full w-2/3 animate-pulse"></div>
                    </div>
                    <p class="text-xs text-gray-400">Please wait, this page will refresh automatically.</p>
                </div>
            @elseif($status === 'completed')
                <div class="mb-8">
                    <div
                        class="w-24 h-24 bg-green-50 rounded-full flex items-center justify-center mx-auto mb-6 border-4 border-green-100">
                        <svg class="w-12 h-12 text-green-500" fill="none" stroke="currentColor" viewBox="0 0 24 24">
                            <path stroke-linecap="round" stroke-linejoin="round" stroke-width="2" d="M5 13l4 4L19 7">
                            </path>
                        </svg>
                    </div>
                    <h2 class="text-2xl font-bold text-[#2d3748] mb-2">Processing Complete!</h2>
                    <p class="text-[#5a6a7a] mb-8">Your reconciliation report is ready.</p>

                    @if (isset($metrics))
                        <div class="grid grid-cols-2 gap-4 mb-8">
                            <div class="bg-blue-50 p-4 rounded-xl border border-blue-100">
                                <p class="text-xs text-blue-600 font-bold uppercase tracking-wider mb-1">Total ATP</p>
                                <p class="text-2xl font-black text-blue-800">{{ number_format($metrics['total_atp']) }}
                                </p>
                            </div>
                            <div class="bg-indigo-50 p-4 rounded-xl border border-indigo-100">
                                <p class="text-xs text-indigo-600 font-bold uppercase tracking-wider mb-1">Total NAP</p>
                                <p class="text-2xl font-black text-indigo-800">
                                    {{ number_format($metrics['total_nap']) }}</p>
                            </div>
                        </div>
                    @endif

                    <a href="{{ $downloadUrl }}"
                        class="flex items-center justify-center gap-2 w-full bg-gradient-to-r from-green-500 to-emerald-600 text-white font-bold py-4 rounded-xl shadow-lg hover:shadow-xl transform hover:-translate-y-1 transition duration-200">
                        <span>📥</span> Download Results (ZIP)
                    </a>
                </div>

                <div class="border-t border-gray-100 pt-6">
                    <a href="{{ route('annual-pass.index') }}"
                        class="text-[#6b9eb8] hover:text-[#4a90a4] text-sm font-semibold flex items-center justify-center gap-1 transition">
                        <span>↺</span> Start New Reconciliation
                    </a>
                </div>
            @elseif($status === 'error')
                <div class="mb-8">
                    <div
                        class="w-24 h-24 bg-red-50 rounded-full flex items-center justify-center mx-auto mb-6 border-4 border-red-100">
                        <svg class="w-12 h-12 text-red-500" fill="none" stroke="currentColor" viewBox="0 0 24 24">
                            <path stroke-linecap="round" stroke-linejoin="round" stroke-width="2"
                                d="M6 18L18 6M6 6l12 12"></path>
                        </svg>
                    </div>
                    <h2 class="text-2xl font-bold text-[#2d3748] mb-2">Processing Failed</h2>
                    <div class="bg-red-50 p-4 rounded-lg border border-red-100 text-red-600 text-sm mb-6 text-left">
                        <strong>Error Details:</strong><br>
                        {{ $message }}
                    </div>
                </div>

                <a href="{{ route('annual-pass.index') }}"
                    class="block w-full bg-gray-100 text-gray-600 font-bold py-3 rounded-xl hover:bg-gray-200 transition">
                    Try Again
                </a>
            @endif

            @if (!empty($logs))
                <div class="mt-10 text-left border-t border-gray-100 pt-6">
                    <details class="group">
                        <summary
                            class="flex items-center justify-between cursor-pointer list-none text-sm font-bold text-gray-400 hover:text-gray-600 transition">
                            <span>VIEW PROCESSING LOGS</span>
                            <span class="transition group-open:rotate-180">
                                <svg class="w-4 h-4" fill="none" stroke="currentColor" viewBox="0 0 24 24">
                                    <path stroke-linecap="round" stroke-linejoin="round" stroke-width="2"
                                        d="M19 9l-7 7-7-7"></path>
                                </svg>
                            </span>
                        </summary>
                        <div
                            class="mt-4 bg-gray-50 p-4 rounded-xl border border-gray-100 font-mono text-[10px] leading-relaxed text-gray-500 overflow-y-auto max-h-48 text-left">
                            @foreach ($logs as $log)
                                <div class="mb-1">{{ $log }}</div>
                            @endforeach
                        </div>
                    </details>
                </div>
            @endif

        </div>
    </div>

</body>

</html>
