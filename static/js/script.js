// Auto-hide alerts after 5 seconds
document.addEventListener('DOMContentLoaded', function() {
    const alerts = document.querySelectorAll('.alert');
    alerts.forEach(function(alert) {
        setTimeout(function() {
            const bsAlert = new bootstrap.Alert(alert);
            bsAlert.close();
        }, 5000);
    });
});

// Add loading spinner to buttons when clicked
document.addEventListener('DOMContentLoaded', function() {
    // Seleccionar todos los botones que estén dentro de un form
    const buttons = document.querySelectorAll('form button[type="submit"], form button[type="button"]');
    
    buttons.forEach(function(button) {
        // Si es un botón tipo "button", añadir el evento al click
        if (button.type === 'button') {
            button.addEventListener('click', function(e) {
                if (!button.form || button.form.checkValidity()) {
                    addSpinnerToButton(button);
                }
            });
        }
        // Si es un botón tipo "submit", añadir el evento al submit del form
        else if (button.type === 'submit') {
            button.form.addEventListener('submit', function(e) {
                if (button.form.checkValidity()) {
                    addSpinnerToButton(button);
                }
            });
        }
    });
});

// Función auxiliar para añadir el spinner
function addSpinnerToButton(button) {
    // Solo añadir el spinner si no existe ya
    if (!button.querySelector('.spinner-border')) {
        const spinner = document.createElement('span');
        spinner.className = 'spinner-border spinner-border-sm ms-2';
        spinner.setAttribute('role', 'status');
        spinner.setAttribute('aria-hidden', 'true');
        button.appendChild(spinner);
        button.disabled = true;
    }
}

// Smooth scroll to top button
document.addEventListener('DOMContentLoaded', function() {
    const scrollButton = document.createElement('button');
    scrollButton.innerHTML = '<i class="fas fa-arrow-up"></i>';
    scrollButton.className = 'btn btn-primary scroll-to-top';
    scrollButton.style.display = 'none';
    document.body.appendChild(scrollButton);

    window.addEventListener('scroll', function() {
        if (window.pageYOffset > 100) {
            scrollButton.style.display = 'block';
        } else {
            scrollButton.style.display = 'none';
        }
    });

    scrollButton.addEventListener('click', function() {
        window.scrollTo({
            top: 0,
            behavior: 'smooth'
        });
    });
});

// Add tooltips to all elements with data-bs-toggle="tooltip"
document.addEventListener('DOMContentLoaded', function() {
    const tooltipTriggerList = [].slice.call(document.querySelectorAll('[data-bs-toggle="tooltip"]'));
    tooltipTriggerList.map(function(tooltipTriggerEl) {
        return new bootstrap.Tooltip(tooltipTriggerEl);
    });
});

// Add confirmation dialog to delete buttons
document.addEventListener('DOMContentLoaded', function() {
    const deleteButtons = document.querySelectorAll('.delete-btn');
    deleteButtons.forEach(function(button) {
        button.addEventListener('click', function(e) {
            if (!confirm('Are you sure you want to delete this item?')) {
                e.preventDefault();
            }
        });
    });
});

// Handle link device form submission
document.addEventListener('DOMContentLoaded', function() {
    const linkForm = document.getElementById('linkForm');
    if (linkForm) {
        console.log('Link form found');
        const submitBtn = linkForm.querySelector('#submitBtn');
        const spinner = submitBtn.querySelector('.spinner-border');
        const buttonText = submitBtn.querySelector('.button-text');

        linkForm.addEventListener('submit', function(e) {
            e.preventDefault();
            console.log('Form submitted');
            
            // Show loading state
            submitBtn.disabled = true;
            spinner.classList.remove('d-none');
            buttonText.textContent = 'Procesando...';
            
            // Get the form data
            const formData = new FormData(linkForm);
            const email = formData.get('email');
            console.log('Email:', email);
            
            // Submit form using fetch
            fetch(linkForm.action, {
                method: 'POST',
                body: formData,
                headers: {
                    'Accept': 'text/html',
                    'X-Requested-With': 'XMLHttpRequest'
                },
                credentials: 'same-origin'
            })
            .then(response => {
                console.log('Response status:', response.status);
                console.log('Response URL:', response.url);
                
                if (response.redirected) {
                    console.log('Redirecting to:', response.url);
                    window.location.href = response.url;
                } else {
                    return response.text().then(html => {
                        if (html.includes('assign_user')) {
                            window.location.href = '/livelyageing/assign';
                        } else {
                            document.documentElement.innerHTML = html;
                        }
                    });
                }
            })
            .catch(error => {
                console.error('Error:', error);
                alert('Error al enviar el formulario. Por favor, inténtalo de nuevo.');
            })
            .finally(() => {
                // Reset button state
                submitBtn.disabled = false;
                spinner.classList.add('d-none');
                buttonText.textContent = 'Autorizar Fitbit';
            });
        });
    }
}); 