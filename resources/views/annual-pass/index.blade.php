<!DOCTYPE html>
<html lang="{{ str_replace('_', '-', app()->getLocale()) }}">

<head>
    <meta charset="utf-8">
    <meta name="viewport" content="width=device-width, initial-scale=1">
    <title>Annual Pass Reconciler</title>
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

        .pipeline-card {
            background: rgba(255, 255, 255, 0.85);
            backdrop-filter: blur(10px);
            border: 1px solid rgba(74, 144, 164, 0.2);
            box-shadow: 0 2px 8px rgba(74, 144, 164, 0.08);
        }

        .pipeline-card:hover {
            transform: translateY(-2px);
            border-color: rgba(74, 144, 164, 0.4);
            box-shadow: 0 8px 32px rgba(74, 144, 164, 0.12);
        }

        .btn-gradient {
            background: linear-gradient(135deg, #6b9eb8 0%, #8ba4c9 100%);
        }

        .btn-gradient:hover {
            transform: translateY(-2px);
            box-shadow: 0 8px 25px rgba(74, 144, 164, 0.3);
        }

        .upload-area {
            border: 2px dashed rgba(74, 144, 164, 0.4);
            background: rgba(74, 144, 164, 0.05);
        }

        .upload-area.dragover {
            background: rgba(74, 144, 164, 0.1);
            border-color: #6b9eb8;
        }
    </style>
</head>

<body class="text-gray-800 p-6 flex flex-col items-center">

    <div class="w-full max-w-4xl space-y-6">

        <!-- Header -->
        <div class="text-center pt-8 pb-4">
            <h1 class="main-header text-5xl font-extrabold mb-2">🚗 Annual Pass Reconciler</h1>
            <p class="text-[#5a6a7a] text-lg">Process toll plaza FASTag ANNUAL PASS transactions with ease</p>
        </div>

        <div class="pipeline-card rounded-2xl p-8 mb-6 transition-all duration-300">
            <h3 class="text-2xl font-semibold text-[#2d3748] mb-6">📤 Upload Transaction Files</h3>

            @if (session('error'))
                <div class="bg-red-50 text-red-700 p-4 rounded-lg mb-6 border border-red-200 font-medium">
                    ❌ {{ session('error') }}
                </div>
            @endif

            <form action="{{ route('annual-pass.process') }}" method="POST" enctype="multipart/form-data"
                id="uploadForm">
                @csrf

                <div class="upload-area rounded-2xl p-10 text-center cursor-pointer relative transition-all duration-200"
                    id="dropZone">
                    <input type="file" name="files[]" multiple required
                        class="absolute inset-0 w-full h-full opacity-0 cursor-pointer z-10" id="fileInput">
                    <div class="pointer-events-none">
                        <div class="mb-4 text-[#6b9eb8]">
                            <svg class="w-16 h-16 mx-auto" fill="none" stroke="currentColor" viewBox="0 0 24 24">
                                <path stroke-linecap="round" stroke-linejoin="round" stroke-width="2"
                                    d="M7 16a4 4 0 01-.88-7.903A5 5 0 1115.9 6L16 6a5 5 0 011 9.9M15 13l-3-3m0 0l-3 3m3-3v12">
                                </path>
                            </svg>
                        </div>
                        <p class="text-xl font-semibold text-[#2d3748]">Drop your Excel or CSV files here</p>
                        <p class="text-sm text-[#5a6a7a] mt-2">Supports .xlsx, .xls, .xlsb, .csv</p>
                    </div>
                </div>

                <!-- File List Display -->
                <div id="fileList" class="mt-6 grid grid-cols-1 md:grid-cols-2 gap-4 hidden">
                    <!-- Files will be injected here via JS -->
                </div>

                <div class="flex gap-6 mt-8">
                    <!-- Sidebar Info (Inline for mobile/web layout adaptation) -->
                    <div class="hidden lg:block w-1/3 space-y-4">
                        <div class="bg-white/50 rounded-xl p-4 border border-blue-100/50">
                            <h4 class="font-bold text-[#2d3748] mb-2 text-sm uppercase">Support</h4>
                            <div class="space-y-2 text-sm text-[#4a5568]">
                                <div class="flex justify-between"><span>ICICI Bank</span> <span class="font-semibold">17
                                        Plazas</span></div>
                                <div class="flex justify-between"><span>IDFC Bank</span> <span class="font-semibold">13
                                        Plazas</span></div>
                            </div>
                        </div>
                    </div>

                    <div class="w-full lg:w-2/3">
                        <button type="submit"
                            class="btn-gradient w-full text-white font-bold py-4 rounded-xl shadow-lg transition duration-200 text-lg flex items-center justify-center gap-2">
                            <span>▶️</span> Run Full Pipeline
                        </button>

                        @if ($errors->any())
                            <div class="text-red-500 text-sm mt-4 bg-red-50 p-3 rounded-lg border border-red-100">
                                @foreach ($errors->all() as $error)
                                    <p>⚠️ {{ $error }}</p>
                                @endforeach
                            </div>
                        @endif
                    </div>
                </div>
            </form>
        </div>

        <div class="grid grid-cols-1 md:grid-cols-3 gap-6 text-[#4a5568] text-sm text-center opacity-80">
            <div class="pipeline-card rounded-xl p-4">
                <div class="font-bold text-lg mb-1">1️⃣ Slicer</div>
                <p>Extracts ANNUALPASS transactions</p>
            </div>
            <div class="pipeline-card rounded-xl p-4">
                <div class="font-bold text-lg mb-1">2️⃣ Merger</div>
                <p>Combines monthly files by project</p>
            </div>
            <div class="pipeline-card rounded-xl p-4">
                <div class="font-bold text-lg mb-1">3️⃣ Reconciler</div>
                <p>Calculates TripCount & Summaries</p>
            </div>
        </div>

    </div>

    <script>
        const fileInput = document.getElementById('fileInput');
        const fileList = document.getElementById('fileList');
        const dropZone = document.getElementById('dropZone');

        // Drag and drop effects
        ['dragenter', 'dragover', 'dragleave', 'drop'].forEach(eventName => {
            dropZone.addEventListener(eventName, preventDefaults, false);
        });

        function preventDefaults(e) {
            e.preventDefault();
            e.stopPropagation();
        }

        ['dragenter', 'dragover'].forEach(eventName => {
            dropZone.addEventListener(eventName, highlight, false);
        });

        ['dragleave', 'drop'].forEach(eventName => {
            dropZone.addEventListener(eventName, unhighlight, false);
        });

        function highlight(e) {
            dropZone.classList.add('dragover');
        }

        function unhighlight(e) {
            dropZone.classList.remove('dragover');
        }

        // File Selection logic
        fileInput.addEventListener('change', updateFileList);

        function updateFileList() {
            const files = fileInput.files;
            if (files.length > 0) {
                fileList.classList.remove('hidden');
                fileList.innerHTML = '';

                // Show up to 6 files, then "+ X more"
                const maxShow = 6;
                Array.from(files).slice(0, maxShow).forEach(file => {
                    const isExcel = file.name.match(/\.(xlsx|xls|xlsb)$/i);
                    const icon = isExcel ? '📊' : '📄';
                    const size = (file.size / 1024).toFixed(1) + ' KB';

                    const div = document.createElement('div');
                    div.className =
                        'bg-[#667eea1a] p-3 rounded-lg border border-[#667eea33] flex items-center gap-3 animate-fade-in';
                    div.innerHTML = `
                        <div class="text-2xl">${icon}</div>
                        <div class="overflow-hidden">
                            <div class="font-semibold text-[#2d3748] truncate" title="${file.name}">${file.name}</div>
                            <div class="text-xs text-[#5a6a7a]">${size}</div>
                        </div>
                    `;
                    fileList.appendChild(div);
                });

                if (files.length > maxShow) {
                    const div = document.createElement('div');
                    div.className =
                        'bg-[#667eea1a] p-3 rounded-lg border border-[#667eea33] flex items-center justify-center font-semibold text-[#5a6a7a]';
                    div.innerText = `+${files.length - maxShow} more files...`;
                    fileList.appendChild(div);
                }
            } else {
                fileList.classList.add('hidden');
            }
        }
    </script>
</body>

</html>
