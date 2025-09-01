const apiGatewayUrl = 'https://b20w5ap291.execute-api.us-east-1.amazonaws.com/generate-upload-url';

const fileInput = document.getElementById('fileInput');
const uploadButton = document.getElementById('uploadButton');
const statusMessage = document.getElementById('statusMessage');

const templateInput = document.getElementById('templateInput');
const templateUploadButton = document.getElementById('templateUploadButton');
const templateStatusMessage = document.getElementById('templateStatusMessage');

// SOP Upload functionality (existing)
uploadButton.addEventListener('click', async () => {
    const file = fileInput.files[0]; 

    if (!file) {
        statusMessage.textContent = 'Please select a file first.';
        statusMessage.className = 'error';
        return;
    }

    uploadButton.disabled = true;
    statusMessage.textContent = 'Requesting upload link...';
    statusMessage.className = '';

    try {
        console.log(`Requesting URL for: ${file.name}, Type: ${file.type}`);

        // Get the pre-signed URL for SOP
        const response = await fetch(apiGatewayUrl, {
            method: 'POST',
            headers: { 'Content-Type': 'application/json' },
            body: JSON.stringify({ 
                filename: file.name,
                contentType: file.type,
                fileCategory: 'sop'
            })
        });

        if (!response.ok) {
            const errorText = await response.text();
            throw new Error(`Could not get upload URL. Status: ${response.status}, Message: ${errorText}`);
        }
        
        const { uploadURL } = await response.json();
        console.log('Received presigned URL. Now uploading...');
        statusMessage.textContent = 'Uploading file...';

        // Upload the file to S3
        const uploadResponse = await fetch(uploadURL, {
            method: 'PUT',
            headers: {
                "Content-Type": file.type
            },
            body: file 
        });

        if (!uploadResponse.ok) {
            const errorText = await uploadResponse.text();
            throw new Error(`File upload failed. Status: ${uploadResponse.status}, Message: ${errorText}`);
        }

        statusMessage.textContent = 'Upload successful! The analysis has started.';
        statusMessage.className = 'success';

    } catch (error) {
        console.error('Upload process failed:', error);
        statusMessage.textContent = `Error: ${error.message}`;
        statusMessage.className = 'error';
    } finally {
        uploadButton.disabled = false;
    }
});

// Template Upload functionality (new)
templateUploadButton.addEventListener('click', async () => {
    const file = templateInput.files[0]; 

    if (!file) {
        templateStatusMessage.textContent = 'Please select an Excel file first.';
        templateStatusMessage.className = 'error';
        return;
    }

    // Validate file type
    const allowedTypes = [
        'application/vnd.openxmlformats-officedocument.spreadsheetml.sheet',
        'application/vnd.ms-excel'
    ];
    
    if (!allowedTypes.includes(file.type)) {
        templateStatusMessage.textContent = 'Please select a valid Excel file (.xlsx or .xls).';
        templateStatusMessage.className = 'error';
        return;
    }

    templateUploadButton.disabled = true;
    templateStatusMessage.textContent = 'Requesting upload link...';
    templateStatusMessage.className = '';

    try {
        console.log(`Requesting URL for template: ${file.name}, Type: ${file.type}`);

        // Get the pre-signed URL for template
        const response = await fetch(apiGatewayUrl, {
            method: 'POST',
            headers: { 'Content-Type': 'application/json' },
            body: JSON.stringify({ 
                filename: file.name,
                contentType: file.type,
                fileCategory: 'template'
            })
        });

        if (!response.ok) {
            const errorText = await response.text();
            throw new Error(`Could not get upload URL. Status: ${response.status}, Message: ${errorText}`);
        }
        
        const { uploadURL } = await response.json();
        console.log('Received presigned URL for template. Now uploading...');
        templateStatusMessage.textContent = 'Uploading template file...';

        // Upload the file to S3
        const uploadResponse = await fetch(uploadURL, {
            method: 'PUT',
            headers: {
                "Content-Type": file.type
            },
            body: file 
        });

        if (!uploadResponse.ok) {
            const errorText = await uploadResponse.text();
            throw new Error(`Template upload failed. Status: ${uploadResponse.status}, Message: ${errorText}`);
        }

        templateStatusMessage.textContent = 'Template uploaded successfully!';
        templateStatusMessage.className = 'success';

    } catch (error) {
        console.error('Template upload process failed:', error);
        templateStatusMessage.textContent = `Error: ${error.message}`;
        templateStatusMessage.className = 'error';
    } finally {
        templateUploadButton.disabled = false;
    }
});
