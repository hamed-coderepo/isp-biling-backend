from django import forms

class FilterForm(forms.Form):
    creators_raw = forms.CharField(
        label='نام ریسلرها (با کاما جدا کنید)',
        required=False,
        widget=forms.TextInput(attrs={
            'class': 'input-main',
            'placeholder': "مثال: Sediqi-Passaband, Another-Reseller"
        })
    )

    filter_serial = forms.BooleanField(required=False, initial=False, label='فیلتر شماره مسلسل',
                                       widget=forms.CheckboxInput(attrs={'class': 'custom-control-input'}))
    serial_op = forms.ChoiceField(
        choices=[('NONE', 'بدون شرط'), ('=', 'برابر با'), ('BETWEEN', 'بین'), ('>', 'بزرگ‌تر از'), ('<', 'کوچک‌تر از')],
        required=False,
        label='نوع شرط',
                widget=forms.Select(attrs={'class': 'select-ui'})
    )
    serial_value = forms.IntegerField(required=False, label='شماره مسلسل (BigQuery)',
                                                                            widget=forms.NumberInput(attrs={'class': 'dynamic-input', 'placeholder': 'مقدار...'}))
    serial_min = forms.IntegerField(required=False, label='از شماره',
                                                                        widget=forms.NumberInput(attrs={'class': 'dynamic-input', 'placeholder': 'از'}))
    serial_max = forms.IntegerField(required=False, label='تا شماره',
                                                                        widget=forms.NumberInput(attrs={'class': 'dynamic-input', 'placeholder': 'تا'}))

    sib_serial_op = forms.ChoiceField(
        choices=[('NONE', 'بدون شرط'), ('=', 'برابر با'), ('BETWEEN', 'بین'), ('>', 'بزرگ‌تر از'), ('<', 'کوچک‌تر از')],
        required=False,
        label='نوع شرط',
                widget=forms.Select(attrs={'class': 'select-ui'})
    )
    sib_serial_value = forms.IntegerField(required=False, label='شماره مسلسل سیب',
                                                                                    widget=forms.NumberInput(attrs={'class': 'dynamic-input', 'placeholder': 'مقدار...'}))
    sib_serial_min = forms.IntegerField(required=False, label='از شماره',
                                                                                widget=forms.NumberInput(attrs={'class': 'dynamic-input', 'placeholder': 'از'}))
    sib_serial_max = forms.IntegerField(required=False, label='تا شماره',
                                                                                widget=forms.NumberInput(attrs={'class': 'dynamic-input', 'placeholder': 'تا'}))

    filter_date = forms.BooleanField(required=False, initial=False, label='فیلتر تاریخ',
                                     widget=forms.CheckboxInput(attrs={'class': 'custom-control-input'}))
    date_op = forms.ChoiceField(
        choices=[('NONE', 'بدون شرط'), ('BETWEEN', 'بین'), ('EXACT', 'دقیق')],
        required=False,
        label='نوع شرط تاریخ',
                widget=forms.Select(attrs={'class': 'select-ui'})
    )
    date_value = forms.DateField(required=False, label='تاریخ',
                                                                 widget=forms.DateInput(attrs={'type': 'date', 'class': 'dynamic-input'}))
    date_start = forms.DateField(required=False, label='از تاریخ',
                                                                 widget=forms.DateInput(attrs={'type': 'date', 'class': 'dynamic-input'}))
    date_end = forms.DateField(required=False, label='تا تاریخ',
                                                             widget=forms.DateInput(attrs={'type': 'date', 'class': 'dynamic-input'}))
